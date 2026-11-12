#include "common.h"

SharedData* shared;
sem_t* dept_mut[MAX_DEPTS];
sem_t* dept_sem[MAX_DEPTS];
sem_t* cust_sem[MAX_CUSTOMERS];

void handle_sigint(int sig) {
    shared->keep_running = 0;
    printf("\n>>> Получен сигнал прерывания (SIGINT). Экстренное завершение работы...\n");
}

int main(int argc, char *argv[]) {
    // Очистка старых ресурсов ядра
    shm_unlink(SHM_NAME);
    unlink(FIFO_NAME);

    // Создание именованного канала (FIFO)
    if (mkfifo(FIFO_NAME, 0666) == -1) {
        perror("mkfifo failed"); return 1;
    }

    // Создание именованной разделяемой памяти
    int shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
    ftruncate(shm_fd, sizeof(SharedData));
    shared = mmap(NULL, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);

    shared->keep_running = 1;
    shared->doors_closed = false;

    // Чтение конфигурации
    const char* config_filename = (argc > 1) ? argv[1] : "config.txt";
    FILE* cfg = fopen(config_filename, "r");
    if (!cfg) { printf("Ошибка конфигурации.\n"); return 1; }

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

        char mut_name[64], sem_name[64];
        sprintf(mut_name, "/shop_dmut_%d", i);
        sprintf(sem_name, "/shop_dsem_%d", i);
        sem_unlink(mut_name); sem_unlink(sem_name);
        dept_mut[i] = sem_open(mut_name, O_CREAT | O_EXCL, 0644, 1);
        dept_sem[i] = sem_open(sem_name, O_CREAT | O_EXCL, 0644, 0);
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
        cust_sem[i] = sem_open(cust_name, O_CREAT | O_EXCL, 0644, 0);
    }
    fclose(cfg);

    // Запуск логгера
    pid_t logger_pid = fork();
    if (logger_pid == 0) {
        int log_fd = open("sim_log.txt", O_CREAT | O_WRONLY | O_TRUNC, 0644);
        int fifo_fd = open(FIFO_NAME, O_RDONLY); // открываем FIFO на чтение
        char buf[1024];
        int n;
        while ((n = read(fifo_fd, buf, sizeof(buf))) > 0) {
            write(STDOUT_FILENO, buf, n);
            if (log_fd != -1) write(log_fd, buf, n);
        }
        exit(0);
    }

    signal(SIGINT, handle_sigint);
    printf("Магазин открыт (Независимые бинарники)...\n");

    // Запуск независимых программ через execl
    pid_t* pids = malloc(sizeof(pid_t) * (shared->num_depts + shared->num_customers));
    int pid_idx = 0;

    for (int i = 0; i < shared->num_depts; i++) {
        if ((pids[pid_idx++] = fork()) == 0) {
            char idx[12], stime[12];
            sprintf(idx, "%d", i); sprintf(stime, "%d", service_time);
            execl("./department", "department", idx, stime, NULL);
            exit(1); // если execl не нашел файл
        }
    }
    for (int i = 0; i < shared->num_customers; i++) {
        if ((pids[pid_idx++] = fork()) == 0) {
            char idx[12];
            sprintf(idx, "%d", i);
            execl("./customer", "customer", idx, NULL);
            exit(1);
        }
    }

    // Таймер
    for (int t = 0; t < workday_duration && shared->keep_running; t++) { usleep(TICK_US); }
    if (shared->keep_running) {
        printf(">>> Время вышло: магазин закрывает двери на вход!\n");
        shared->doors_closed = true;
    }

    // Ожидание и очистка
    for (int i = 0; i < shared->num_customers; i++) sem_post(cust_sem[i]);
    shared->keep_running = 0;
    for (int i = 0; i < shared->num_depts; i++) sem_post(dept_sem[i]);

    for (int i = 0; i < pid_idx; i++) waitpid(pids[i], NULL, 0);
    printf("Все покупатели обслужены. Магазин закрыт. Программа завершена.\n");

    // Остановка логгера (запись в FIFO закрыта всеми клиентами)
    waitpid(logger_pid, NULL, 0);

    for (int i = 0; i < shared->num_depts; i++) {
        char mname[64], sname[64];
        sprintf(mname, "/shop_dmut_%d", i); sprintf(sname, "/shop_dsem_%d", i);
        sem_close(dept_mut[i]); sem_unlink(mname);
        sem_close(dept_sem[i]); sem_unlink(sname);
    }
    for (int i = 0; i < shared->num_customers; i++) {
        char cname[64];
        sprintf(cname, "/shop_csem_%d", i);
        sem_close(cust_sem[i]); sem_unlink(cname);
    }

    munmap(shared, sizeof(SharedData));
    shm_unlink(SHM_NAME);
    unlink(FIFO_NAME);
    free(pids);

    return 0;
}