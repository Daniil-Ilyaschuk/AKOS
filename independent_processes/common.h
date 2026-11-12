#ifndef АКОС_COMMON_H
#define АКОС_COMMON_H

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
#define MAX_DEPTS 10
#define MAX_CUSTOMERS 100
#define MAX_ITEMS 20
#define MAX_QUEUE 50

#define SHM_NAME "/shop_shared_mem"
#define FIFO_NAME "/tmp/shop_log_fifo"

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
    int queue[MAX_QUEUE];
    int queue_size;
} Department;

typedef struct {
    Department depts[MAX_DEPTS];
    Customer customers[MAX_CUSTOMERS];
    int num_depts;
    int num_customers;
    volatile sig_atomic_t keep_running;
    volatile bool doors_closed;
} SharedData;

// Вспомогательная функция логирования в именованный канал (FIFO)
static inline void sys_log(int fifo_fd, const char *format, ...) {
    char buffer[256];
    va_list args;
    va_start(args, format);
    int len = vsnprintf(buffer, sizeof(buffer), format, args);
    va_end(args);
    write(fifo_fd, buffer, len);
}

// Потокобезопасные функции очередей
static inline void enqueue(Department* dept, int cust_idx) {
    if (dept->queue_size < MAX_QUEUE) {
        dept->queue[dept->queue_size++] = cust_idx;
    }
}

static inline int dequeue(Department* dept) {
    if (dept->queue_size == 0) return -1;
    int cust_idx = dept->queue[0];
    for (int i = 1; i < dept->queue_size; i++) {
        dept->queue[i - 1] = dept->queue[i];
    }
    dept->queue_size--;
    return cust_idx;
}

static inline int get_dept_index_for_item(SharedData* shared, int item_id) {
    for (int i = 0; i < shared->num_depts; i++) {
        for (int j = 0; j < shared->depts[i].assortment_size; j++) {
            if (shared->depts[i].assortment[j] == item_id) return i;
        }
    }
    return -1;
}

#endif