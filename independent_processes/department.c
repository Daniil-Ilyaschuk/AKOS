#include "common.h"

int main(int argc, char* argv[]) {
    if (argc < 3) return 1;
    signal(SIGINT, SIG_IGN); // управляется менеджером

    int dept_idx = atoi(argv[1]);
    int service_time = atoi(argv[2]);

    // Подключение к именованной разделяемой памяти (shm_open)
    int shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
    SharedData* shared = mmap(NULL, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

    // Подключение к именованному каналу для логов (FIFO)
    int fifo_fd = open(FIFO_NAME, O_WRONLY);

    char mut_name[64], sem_name[64];
    sprintf(mut_name, "/shop_dmut_%d", dept_idx);
    sprintf(sem_name, "/shop_dsem_%d", dept_idx);
    sem_t* mut = sem_open(mut_name, 0);
    sem_t* sem = sem_open(sem_name, 0);

    sem_t* cust_sem[MAX_CUSTOMERS];
    for(int i = 0; i < shared->num_customers; i++) {
        char cname[64];
        sprintf(cname, "/shop_csem_%d", i);
        cust_sem[i] = sem_open(cname, 0);
    }

    Department* d = &shared->depts[dept_idx];

    while (shared->keep_running) {
        sem_wait(sem);
        if (!shared->keep_running) break;

        sem_wait(mut);
        int cust_idx = dequeue(d);
        if (d->queue_size == 0) {
            sys_log(fifo_fd, "Продавец отдела %d засыпает из-за отсутствия покупателей.\n", d->id);
            d->is_sleeping = true;
        }
        sem_post(mut);

        if (cust_idx != -1) {
            for (int i = 0; i < service_time && shared->keep_running; i++) usleep(TICK_US);
            sem_post(cust_sem[cust_idx]);
        }
    }

    close(fifo_fd);
    munmap(shared, sizeof(SharedData));
    return 0;
}