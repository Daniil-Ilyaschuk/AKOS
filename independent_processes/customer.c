#include "common.h"

int main(int argc, char* argv[]) {
    if (argc < 2) return 1;
    signal(SIGINT, SIG_IGN);

    int cust_idx = atoi(argv[1]);

    int shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
    SharedData* shared = mmap(NULL, sizeof(SharedData), PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    int fifo_fd = open(FIFO_NAME, O_WRONLY);

    sem_t* dept_mut[MAX_DEPTS];
    sem_t* dept_sem[MAX_DEPTS];
    for (int i = 0; i < shared->num_depts; i++) {
        char mname[64], sname[64];
        sprintf(mname, "/shop_dmut_%d", i);
        sprintf(sname, "/shop_dsem_%d", i);
        dept_mut[i] = sem_open(mname, 0);
        dept_sem[i] = sem_open(sname, 0);
    }

    char cname[64];
    sprintf(cname, "/shop_csem_%d", cust_idx);
    sem_t* my_sem = sem_open(cname, 0);

    Customer* c = &shared->customers[cust_idx];

    for (int i = 0; i < c->arrival_tick && shared->keep_running; i++) usleep(TICK_US);

    if (!shared->keep_running) { munmap(shared, sizeof(SharedData)); return 0; }

    if (shared->doors_closed) {
        sys_log(fifo_fd, "Покупатель %d пришел после закрытия и разворачивается.\n", c->id);
        munmap(shared, sizeof(SharedData)); return 0;
    }

    sys_log(fifo_fd, "Покупатель %d вошел в магазин.\n", c->id);

    for (int i = 0; i < c->list_size && shared->keep_running; i++) {
        int item = c->shopping_list[i];
        int target_idx = get_dept_index_for_item(shared, item);

        if (target_idx == -1) continue;
        Department* target_dept = &shared->depts[target_idx];

        sys_log(fifo_fd, "Покупатель %d направляется в отдел %d за товаром %d\n", c->id, target_dept->id, item);

        sem_wait(dept_mut[target_idx]);
        enqueue(target_dept, cust_idx);
        sys_log(fifo_fd, "Покупатель %d встает в очередь.\n", c->id);

        if (target_dept->is_sleeping) {
            sys_log(fifo_fd, "Продавец отдела %d просыпается!\n", target_dept->id);
            target_dept->is_sleeping = false;
        }
        sem_post(dept_mut[target_idx]);

        sem_post(dept_sem[target_idx]);
        sem_wait(my_sem);

        if (shared->keep_running) {
            sys_log(fifo_fd, "Покупатель %d покупает товар %d в отделе %d\n", c->id, item, target_dept->id);
        }
    }

    if (shared->keep_running) {
        sys_log(fifo_fd, "Покупатель %d завершил список и покидает магазин.\n", c->id);
    }

    close(fifo_fd);
    munmap(shared, sizeof(SharedData));
    return 0;
}