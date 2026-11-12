#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <fcntl.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/stat.h>

#define TICK_US 400000

// Добавляем константы лимитов для упрощения размещения в разделяемой памяти
#define MAX_DEPTS 10
#define MAX_CUSTOMERS 100
#define MAX_ITEMS 20
#define MAX_QUEUE 50

typedef struct {
    int id;
    int shopping_list[MAX_ITEMS];
    int list_size;
    int arrival_tick;
} Customer;

typedef struct {
    int id;
    int assortment[MAX_ITEMS];
    int assortment_size;
    bool is_sleeping;
    int queue[MAX_QUEUE]; // хранит индексы покупателей
    int queue_size;
} Department;

// Единая структура, которая будет лежать в разделяемой памяти
typedef struct {
    Department depts[MAX_DEPTS];
    Customer customers[MAX_CUSTOMERS];
    int num_depts;
    int num_customers;
    volatile sig_atomic_t keep_running;
    volatile bool doors_closed;
} SharedData;

SharedData* shared;
int log_pipe[2]; // pipe для логов

// Глобальные массивы для хранения семафоров в памяти родительского процесса
sem_t* dept_mut[MAX_DEPTS];
sem_t* dept_sem[MAX_DEPTS];
sem_t* cust_sem[MAX_CUSTOMERS];

// Функция логирования для дочерних процессов (пишет в Pipe)
void sys_log(const char *format, ...) {
    char buffer[256];
    va_list args;
    va_start(args, format);
    int len = vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);

    // Атомарная запись в канал
    write(log_pipe[1], buffer, len);
}

// Обработчик сигналов только для родительского процесса
void handle_sigint(int sig) {
    shared->keep_running = 0;
    sys_log("\n>>> Получен сигнал прерывания (SIGINT). Экстренное завершение работы...\n");
}

// Потокобезопасные функции очередей
void enqueue(Department* dept, int cust_idx) {
    if (dept->queue_size < MAX_QUEUE) {
        dept->queue[dept->queue_size++] = cust_idx;
    }
}

int dequeue(Department* dept) {
    if (dept->queue_size == 0) return -1;
    int cust_idx = dept->queue[0];
    for (int i = 1; i < dept->queue_size; i++) {
        dept->queue[i - 1] = dept->queue[i];
    }
    dept->queue_size--;
    return cust_idx;
}

int get_dept_index_for_item(int item_id) {
    for (int i = 0; i < shared->num_depts; i++) {
        for (int j = 0; j < shared->depts[i].assortment_size; j++) {
            if (shared->depts[i].assortment[j] == item_id) return i;
        }
    }
    return -1;
}

// Дочерние процессы
void department_process(int dept_idx, int service_time) {
    // Ctrl+C игнорируется, родитель сам управляет их завершением
    signal(SIGINT, SIG_IGN);
    Department* d = &shared->depts[dept_idx];

    while (shared->keep_running) {
        // Ожидание появления покупателя в очереди
        sem_wait(dept_sem[dept_idx]);
        if (!shared->keep_running) break;

        sem_wait(dept_mut[dept_idx]); // блокировка очереди (мьютекс)
        int cust_idx = dequeue(d);
        if (d->queue_size == 0) {
            sys_log("Продавец отдела %d засыпает из-за отсутствия покупателей.\n", d->id);
            d->is_sleeping = true;
        }
        sem_post(dept_mut[dept_idx]);

        if (cust_idx != -1) {
            for (int i = 0; i < service_time && shared->keep_running; i++) usleep(TICK_US);

            // Будим покупателя, товар выдан
            sem_post(cust_sem[cust_idx]);
        }
    }
    exit(0);
}

void customer_process(int cust_idx) {
    signal(SIGINT, SIG_IGN);
    Customer* c = &shared->customers[cust_idx];

    for (int i = 0; i < c->arrival_tick && shared->keep_running; i++) usleep(TICK_US);

    if (!shared->keep_running) exit(0);

    if (shared->doors_closed) {
        sys_log("Покупатель %d пришел после закрытия и разворачивается.\n", c->id);
        exit(0);
    }

    sys_log("Покупатель %d вошел в магазин.\n", c->id);

    for (int i = 0; i < c->list_size && shared->keep_running; i++) {
        int item = c->shopping_list[i];
        int target_idx = get_dept_index_for_item(item);

        if (target_idx == -1) continue;
        Department* target_dept = &shared->depts[target_idx];

        sys_log("Покупатель %d направляется в отдел %d за товаром %d\n", c->id, target_dept->id, item);

        sem_wait(dept_mut[target_idx]);
        enqueue(target_dept, cust_idx);
        sys_log("Покупатель %d встает в очередь.\n", c->id);

        if (target_dept->is_sleeping) {
            sys_log("Продавец отдела %d просыпается!\n", target_dept->id);
            target_dept->is_sleeping = false;
        }
        sem_post(dept_mut[target_idx]);

        sem_post(dept_sem[target_idx]); // сигналим продавцу
        sem_wait(cust_sem[cust_idx]);   // ждем у кассы

        if (shared->keep_running) {
            sys_log("Покупатель %d покупает товар %d в отделе %d\n", c->id, item, target_dept->id);
        }
    }

    if (shared->keep_running) {
        sys_log("Покупатель %d завершил список и покидает магазин.\n", c->id);
    }
    exit(0);
}

// Родительский процесс
int main(int argc, char *argv[]) {
    // Настройка канала для логов
    if (pipe(log_pipe) == -1) {
        perror("Pipe failed");
        return 1;
    }

    int log_fd = open("sim_log.txt", O_CREAT | O_WRONLY | O_TRUNC, 0644);

    // Выделение разделяемой памяти (mmap)
    shared = mmap(NULL, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANON, -1, 0);
    shared->keep_running = 1;
    shared->doors_closed = false;

    // Чтение конфигурации
    const char* config_filename = (argc > 1) ? argv[1] : "config.txt";
    FILE* cfg = fopen(config_filename, "r");
    if (!cfg) {
        printf("Ошибка: не удалось открыть конфигурационный файл.\n");
        return 1;
    }

    int service_time, workday_duration;
    fscanf(cfg, "%d %d", &service_time, &workday_duration);

    fscanf(cfg, "%d", &shared->num_depts);
    for (int i = 0; i < shared->num_depts; i++) {
        fscanf(cfg, "%d %d", &shared->depts[i].id, &shared->depts[i].assortment_size);
        for (int j = 0; j < shared->depts[i].assortment_size; j++) {
            fscanf(cfg, "%d", &shared->depts[i].assortment[j]);
        }
        shared->depts[i].is_sleeping = true;
        shared->depts[i].queue_size = 0;

        // Создание именованных семафоров для отдела
        char mut_name[64], sem_name[64];
        sprintf(mut_name, "/shop_dmut_%d", i);
        sprintf(sem_name, "/shop_dsem_%d", i);

        sem_unlink(mut_name);
        sem_unlink(sem_name);
        dept_mut[i] = sem_open(mut_name, O_CREAT | O_EXCL, 0644, 1); // 1 = мьютекс открыт
        dept_sem[i] = sem_open(sem_name, O_CREAT | O_EXCL, 0644, 0); // 0 = продавец спит
    }

    fscanf(cfg, "%d", &shared->num_customers);
    for (int i = 0; i < shared->num_customers; i++) {
        fscanf(cfg, "%d %d %d", &shared->customers[i].id, &shared->customers[i].arrival_tick, &shared->customers[i].list_size);
        for (int j = 0; j < shared->customers[i].list_size; j++) {
            fscanf(cfg, "%d", &shared->customers[i].shopping_list[j]);
        }

        char cust_name[64];
        sprintf(cust_name, "/shop_csem_%d", i);
        sem_unlink(cust_name);
        cust_sem[i] = sem_open(cust_name, O_CREAT | O_EXCL, 0644, 0); // 0 = покупатель ждет
    }
    fclose(cfg);

    // Запуск процесса-логгера
    pid_t logger_pid = fork();
    if (logger_pid == 0) {
        close(log_pipe[1]); // закрываем конец для записи
        char buf[1024];
        int n;
        while ((n = read(log_pipe[0], buf, sizeof(buf))) > 0) {
            write(STDOUT_FILENO, buf, n);
            if (log_fd != -1) write(log_fd, buf, n);
        }
        exit(0);
    }
    close(log_pipe[0]); // основной и дочерние процессы закрывают конец для чтения

    // Перехват сигналов после инициализации памяти и логгера
    signal(SIGINT, handle_sigint);

    // Запуск дочерних процессов
    sys_log("Магазин открыт. Начинается симуляция (Многопроцессность)...\n");

    pid_t* dept_pids = malloc(sizeof(pid_t) * shared->num_depts);
    for (int i = 0; i < shared->num_depts; i++) {
        if ((dept_pids[i] = fork()) == 0) {
            department_process(i, service_time); // ребенок уходит в функцию и завершается
        }
    }

    pid_t* cust_pids = malloc(sizeof(pid_t) * shared->num_customers);
    for (int i = 0; i < shared->num_customers; i++) {
        if ((cust_pids[i] = fork()) == 0) {
            customer_process(i);
        }
    }

    // Таймер магазина (Родительский процесс)
    for (int t = 0; t < workday_duration && shared->keep_running; t++) {
        usleep(TICK_US);
    }

    if (shared->keep_running) {
        sys_log(">>> Время вышло: магазин закрывает двери на вход!\n");
        shared->doors_closed = true;
    }

    // Ожидание завершения и очистка
    for (int i = 0; i < shared->num_customers; i++) {
        sem_post(cust_sem[i]); // экстренная побудка
        waitpid(cust_pids[i], NULL, 0);
    }

    sys_log("Все покупатели обслужены. Магазин закрыт. Программа завершена.\n");
    shared->keep_running = 0;

    for (int i = 0; i < shared->num_depts; i++) {
        sem_post(dept_sem[i]);
        waitpid(dept_pids[i], NULL, 0);
    }

    // Закрываем трубу, логгер прочитает EOF и завершится
    close(log_pipe[1]);
    waitpid(logger_pid, NULL, 0);

    // Удаление именованных семафоров из системы
    for (int i = 0; i < shared->num_depts; i++) {
        char mut_name[64], sem_name[64];
        sprintf(mut_name, "/shop_dmut_%d", i);
        sprintf(sem_name, "/shop_dsem_%d", i);
        sem_close(dept_mut[i]); sem_unlink(mut_name);
        sem_close(dept_sem[i]); sem_unlink(sem_name);
    }
    for (int i = 0; i < shared->num_customers; i++) {
        char cust_name[64];
        sprintf(cust_name, "/shop_csem_%d", i);
        sem_close(cust_sem[i]); sem_unlink(cust_name);
    }

    munmap(shared, sizeof(SharedData));
    free(dept_pids);
    free(cust_pids);
    if (log_fd != -1) close(log_fd);

    return 0;
}