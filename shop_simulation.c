#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>

#define TICK_US 400000

int log_fd = -1;
volatile sig_atomic_t keep_running = 1;
volatile bool doors_closed = false;
int service_time = 0;

void sys_log(const char *format, ...) {
    char buffer[256];
    va_list args;
    va_start(args, format);
    int len = vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);

    write(STDOUT_FILENO, buffer, len);
    if (log_fd != -1) write(log_fd, buffer, len);
}

void handle_sigint(int sig) {
    keep_running = 0;
    sys_log("\n>>> Получен сигнал прерывания (SIGINT). Экстренное завершение работы...\n");
}

typedef struct {
    int id;
    int* shopping_list;
    int list_size;
    int arrival_tick;

    // Синхронизация покупателя через условные переменные
    pthread_mutex_t wait_mutex;
    pthread_cond_t wait_cond;
    bool is_served;
} Customer;

typedef struct {
    int id;
    int* assortment;
    int assortment_size;
    bool is_sleeping;
    Customer** queue;
    int queue_size;
    int queue_capacity;

    // Синхронизация очереди отдела через условные переменные
    pthread_mutex_t queue_mutex;
    pthread_cond_t customer_cond;
} Department;

Department* depts;
int num_depts;

void enqueue(Department* dept, Customer* c) {
    if (dept->queue_size < dept->queue_capacity) {
        dept->queue[dept->queue_size++] = c;
    }
}

Customer* dequeue(Department* dept) {
    if (dept->queue_size == 0) return NULL;
    Customer* c = dept->queue[0];
    for (int i = 1; i < dept->queue_size; i++) {
        dept->queue[i - 1] = dept->queue[i];
    }
    dept->queue_size--;
    return c;
}

int get_dept_index_for_item(int item_id) {
    for (int i = 0; i < num_depts; i++) {
        for (int j = 0; j < depts[i].assortment_size; j++) {
            if (depts[i].assortment[j] == item_id) return i;
        }
    }
    return -1;
}

// Поток продавца
void* department_worker(void* arg) {
    Department* d = (Department*)arg;

    while (keep_running) {
        pthread_mutex_lock(&d->queue_mutex);

        // Сон на условной переменной, пока очередь пуста
        while (d->queue_size == 0 && keep_running) {
            d->is_sleeping = true;
            pthread_cond_wait(&d->customer_cond, &d->queue_mutex);
        }

        if (!keep_running) {
            pthread_mutex_unlock(&d->queue_mutex);
            break;
        }

        Customer* c = dequeue(d);
        if (d->queue_size == 0) {
            sys_log("Продавец отдела %d засыпает из-за отсутствия покупателей.\n", d->id);
            d->is_sleeping = true;
        }
        pthread_mutex_unlock(&d->queue_mutex);

        if (c != NULL) {
            for (int i = 0; i < service_time && keep_running; i++) usleep(TICK_US);

            // Сообщаем покупателю, что товар выдан
            pthread_mutex_lock(&c->wait_mutex);
            c->is_served = true;
            pthread_cond_signal(&c->wait_cond);
            pthread_mutex_unlock(&c->wait_mutex);
        }
    }
    return NULL;
}

// Поток покупателя
void* customer_worker(void* arg) {
    Customer* c = (Customer*)arg;

    for (int i = 0; i < c->arrival_tick && keep_running; i++) usleep(TICK_US);

    if (!keep_running) return NULL;

    if (doors_closed) {
        sys_log("Покупатель %d пришел после закрытия и разворачивается.\n", c->id);
        return NULL;
    }

    sys_log("Покупатель %d вошел в магазин.\n", c->id);

    for (int i = 0; i < c->list_size && keep_running; i++) {
        int item = c->shopping_list[i];
        int dept_idx = get_dept_index_for_item(item);

        if (dept_idx == -1) continue;
        Department* target_dept = &depts[dept_idx];

        sys_log("Покупатель %d направляется в отдел %d за товаром %d\n", c->id, target_dept->id, item);

        // Встаем в очередь и будим продавца
        pthread_mutex_lock(&target_dept->queue_mutex);
        enqueue(target_dept, c);
        sys_log("Покупатель %d встает в очередь.\n", c->id);
        if (target_dept->is_sleeping) {
            sys_log("Продавец отдела %d просыпается!\n", target_dept->id);
            target_dept->is_sleeping = false;
        }
        pthread_cond_signal(&target_dept->customer_cond);
        pthread_mutex_unlock(&target_dept->queue_mutex);

        // Ждем обслуживания у кассы
        pthread_mutex_lock(&c->wait_mutex);
        while (!c->is_served && keep_running) {
            pthread_cond_wait(&c->wait_cond, &c->wait_mutex);
        }
        c->is_served = false;
        pthread_mutex_unlock(&c->wait_mutex);

        if (keep_running) {
            sys_log("Покупатель %d покупает товар %d в отделе %d\n", c->id, item, target_dept->id);
        }
    }

    if (keep_running) {
        sys_log("Покупатель %d завершил список и покидает магазин.\n", c->id);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    signal(SIGINT, handle_sigint);

    log_fd = open("sim_log.txt", O_CREAT | O_WRONLY | O_TRUNC, 0644);

    const char* config_filename = (argc > 1) ? argv[1] : "config.txt";
    FILE* cfg = fopen(config_filename, "r");
    if (!cfg) {
        sys_log("Ошибка: не удалось открыть конфигурационный файл.\n");
        return 1;
    }

    int workday_duration;
    fscanf(cfg, "%d %d", &service_time, &workday_duration);

    sys_log("Магазин открыт. Начинается симуляция...\n");

    fscanf(cfg, "%d", &num_depts);
    depts = malloc(sizeof(Department) * num_depts);
    pthread_t* dept_threads = malloc(sizeof(pthread_t) * num_depts);

    for (int i = 0; i < num_depts; i++) {
        fscanf(cfg, "%d %d", &depts[i].id, &depts[i].assortment_size);
        depts[i].assortment = malloc(sizeof(int) * depts[i].assortment_size);
        for (int j = 0; j < depts[i].assortment_size; j++) {
            fscanf(cfg, "%d", &depts[i].assortment[j]);
        }
        depts[i].is_sleeping = true;
        depts[i].queue_capacity = 20;
        depts[i].queue_size = 0;
        depts[i].queue = malloc(sizeof(Customer*) * depts[i].queue_capacity);

        pthread_mutex_init(&depts[i].queue_mutex, NULL);
        pthread_cond_init(&depts[i].customer_cond, NULL);

        pthread_create(&dept_threads[i], NULL, department_worker, &depts[i]);
    }

    int num_customers;
    fscanf(cfg, "%d", &num_customers);
    Customer* customers = malloc(sizeof(Customer) * num_customers);
    pthread_t* customer_threads = malloc(sizeof(pthread_t) * num_customers);

    for (int i = 0; i < num_customers; i++) {
        fscanf(cfg, "%d %d %d", &customers[i].id, &customers[i].arrival_tick, &customers[i].list_size);
        customers[i].shopping_list = malloc(sizeof(int) * customers[i].list_size);
        for (int j = 0; j < customers[i].list_size; j++) {
            fscanf(cfg, "%d", &customers[i].shopping_list[j]);
        }

        pthread_mutex_init(&customers[i].wait_mutex, NULL);
        pthread_cond_init(&customers[i].wait_cond, NULL);
        customers[i].is_served = false;

        pthread_create(&customer_threads[i], NULL, customer_worker, &customers[i]);
    }
    fclose(cfg);

    for (int current_tick = 0; current_tick < workday_duration && keep_running; current_tick++) {
        usleep(TICK_US);
    }

    if (keep_running) {
        sys_log(">>> Время вышло: магазин закрывает двери на вход!\n");
        doors_closed = true;
    }

    for (int i = 0; i < num_customers; i++) {
        // Если день закончился, разблокируем покупателей, которые не успели войти
        pthread_cond_broadcast(&customers[i].wait_cond);
        pthread_join(customer_threads[i], NULL);
    }

    sys_log("Все покупатели обслужены. Магазин закрыт. Программа завершена.\n");

    keep_running = false;
    for (int i = 0; i < num_depts; i++) {
        pthread_mutex_lock(&depts[i].queue_mutex);
        pthread_cond_broadcast(&depts[i].customer_cond); // Экстренная побудка для завершения
        pthread_mutex_unlock(&depts[i].queue_mutex);
        pthread_join(dept_threads[i], NULL);
    }

    for (int i = 0; i < num_depts; i++) {
        pthread_mutex_destroy(&depts[i].queue_mutex);
        pthread_cond_destroy(&depts[i].customer_cond);
        free(depts[i].assortment);
        free(depts[i].queue);
    }
    free(depts);
    free(dept_threads);

    for (int i = 0; i < num_customers; i++) {
        pthread_mutex_destroy(&customers[i].wait_mutex);
        pthread_cond_destroy(&customers[i].wait_cond);
        free(customers[i].shopping_list);
    }
    free(customers);
    free(customer_threads);

    if (log_fd != -1) close(log_fd);
    return 0;
}