#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <fcntl.h>

int log_fd = -1; // Глобальный дескриптор файла лога

void sys_print(const char* msg) {
    write(STDOUT_FILENO, msg, strlen(msg));
    if (log_fd != -1) write(log_fd, msg, strlen(msg));
}

void sys_print_num(const char* prefix, int num, const char* suffix) {
    char buffer[256];
    snprintf(buffer, sizeof(buffer), "%s%d%s", prefix, num, suffix);
    write(STDOUT_FILENO, buffer, strlen(buffer));
    if (log_fd != -1) write(log_fd, buffer, strlen(buffer));
}

typedef enum {
    STATE_WAITING_TO_ENTER,
    STATE_ARRIVING,
    STATE_QUEUEING,
    STATE_BUYING,
    STATE_MOVING,
    STATE_DONE
} CustomerState;

typedef struct {
    int id;
    int* shopping_list;
    int list_size;
    int current_item_index;
    CustomerState state;
    int arrival_tick;
} Customer;

typedef struct {
    int id;
    int* assortment;
    int assortment_size;
    bool is_sleeping;
    Customer* current_customer;
    int service_time_left;
    Customer** queue;
    int queue_size;
    int queue_capacity;
} Department;

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

int get_dept_index_for_item(int item_id, Department* depts, int num_depts) {
    for (int i = 0; i < num_depts; i++) {
        for (int j = 0; j < depts[i].assortment_size; j++) {
            if (depts[i].assortment[j] == item_id) return i;
        }
    }
    return -1;
}

int main(int argc, char *argv[]) {
    // Открытие файла журнала для записи (создание или очистка)
    log_fd = open("sim_log.txt", O_CREAT | O_WRONLY | O_TRUNC, 0644);

    const char* config_filename = (argc > 1) ? argv[1] : "config.txt";
    FILE* cfg = fopen(config_filename, "r");
    if (!cfg) {
        sys_print("Ошибка: не удалось открыть конфигурационный файл.\n");
        return 1;
    }

    int service_time, workday_duration;
    fscanf(cfg, "%d %d", &service_time, &workday_duration);

    int num_depts;
    fscanf(cfg, "%d", &num_depts);
    Department* depts = malloc(sizeof(Department) * num_depts);

    for (int i = 0; i < num_depts; i++) {
        fscanf(cfg, "%d %d", &depts[i].id, &depts[i].assortment_size);
        depts[i].assortment = malloc(sizeof(int) * depts[i].assortment_size);
        for (int j = 0; j < depts[i].assortment_size; j++) {
            fscanf(cfg, "%d", &depts[i].assortment[j]);
        }
        depts[i].is_sleeping = true;
        depts[i].current_customer = NULL;
        depts[i].service_time_left = 0;
        depts[i].queue_capacity = 20;
        depts[i].queue_size = 0;
        depts[i].queue = malloc(sizeof(Customer*) * depts[i].queue_capacity);
    }

    int num_customers;
    fscanf(cfg, "%d", &num_customers);
    Customer* customers = malloc(sizeof(Customer) * num_customers);

    for (int i = 0; i < num_customers; i++) {
        fscanf(cfg, "%d %d %d", &customers[i].id, &customers[i].arrival_tick, &customers[i].list_size);
        customers[i].shopping_list = malloc(sizeof(int) * customers[i].list_size);
        for (int j = 0; j < customers[i].list_size; j++) {
            fscanf(cfg, "%d", &customers[i].shopping_list[j]);
        }
        customers[i].current_item_index = 0;
        customers[i].state = STATE_WAITING_TO_ENTER;
    }
    fclose(cfg);

    int active_customers = num_customers;
    sys_print("Магазин открыт. Начинается симуляция...\n");
    int current_tick = 0;
    bool doors_closed = false;

    while (active_customers > 0) {
        if (current_tick >= workday_duration && !doors_closed) {
            sys_print(">>> Время вышло: магазин закрывает двери на вход!\n");
            doors_closed = true;
        }

        // ФАЗА 1: Продавцы завершают обслуживание
        for (int i = 0; i < num_depts; i++) {
            if (depts[i].current_customer != NULL) {
                depts[i].service_time_left--;
                if (depts[i].service_time_left <= 0) {
                    Customer* c = depts[i].current_customer;
                    int item = c->shopping_list[c->current_item_index];

                    sys_print_num("Покупатель ", c->id, " покупает товар ");
                    sys_print_num("", item, " в отделе ");
                    sys_print_num("", depts[i].id, "\n");

                    c->current_item_index++;
                    depts[i].current_customer = NULL;

                    if (c->current_item_index >= c->list_size) {
                        sys_print_num("Покупатель ", c->id, " завершил список и покидает магазин.\n");
                        c->state = STATE_DONE;
                        active_customers--;
                    } else {
                        c->state = STATE_MOVING;
                    }
                }
            }
        }

        // ФАЗА 2: Покупатели принимают решения и перемещаются
        for (int i = 0; i < num_customers; i++) {
            Customer* c = &customers[i];
            if (c->state == STATE_DONE) continue;

            if (c->state == STATE_WAITING_TO_ENTER && current_tick == c->arrival_tick) {
                if (doors_closed) {
                    sys_print_num("Покупатель ", c->id, " пришел после закрытия и разворачивается.\n");
                    c->state = STATE_DONE;
                    active_customers--;
                    continue;
                }
                sys_print_num("Покупатель ", c->id, " вошел в магазин. Список: ");
                for (int j = 0; j < c->list_size; j++) {
                    sys_print_num("", c->shopping_list[j], j < c->list_size - 1 ? ", " : "\n");
                }
                c->state = STATE_MOVING;
            }

            if (c->state == STATE_MOVING) {
                int next_item = c->shopping_list[c->current_item_index];
                int target_dept_idx = get_dept_index_for_item(next_item, depts, num_depts);

                if (target_dept_idx != -1) {
                    Department* target_dept = &depts[target_dept_idx];
                    sys_print_num("Покупатель ", c->id, " направляется в отдел ");
                    sys_print_num("", target_dept->id, " за товаром ");
                    sys_print_num("", next_item, "\n");

                    enqueue(target_dept, c);
                    sys_print_num("Покупатель ", c->id, " встает в очередь.\n");
                    c->state = STATE_QUEUEING;

                    if (target_dept->is_sleeping) {
                        sys_print_num("Продавец отдела ", target_dept->id, " просыпается!\n");
                        target_dept->is_sleeping = false;
                    }
                }
            }
        }

        // ФАЗА 3: Свободные продавцы начинают обслуживание или засыпают
        for (int i = 0; i < num_depts; i++) {
            if (depts[i].current_customer == NULL) {
                if (depts[i].queue_size > 0) {
                    depts[i].current_customer = dequeue(&depts[i]);
                    depts[i].current_customer->state = STATE_BUYING;
                    depts[i].service_time_left = service_time;
                } else if (!depts[i].is_sleeping) {
                    sys_print_num("Продавец отдела ", depts[i].id, " засыпает из-за отсутствия покупателей.\n");
                    depts[i].is_sleeping = true;
                }
            }
        }

        usleep(400000);
        current_tick++;
    }

    sys_print("Магазин закрыт. Программа завершена.\n");

    // Очистка памяти
    for (int i = 0; i < num_depts; i++) {
        free(depts[i].assortment);
        free(depts[i].queue);
    }
    free(depts);
    for (int i = 0; i < num_customers; i++) {
        free(customers[i].shopping_list);
    }
    free(customers);

    if (log_fd != -1) close(log_fd);
    return 0;
}