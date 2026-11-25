#include "protocol.h"

static volatile sig_atomic_t running = 1;

static void handle_signal(int sig) {
    (void)sig;
    running = 0;
}

static int recv_interruptible(int fd, Packet *pkt) {
    while (running) {
        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int rc = poll(&pfd, 1, 200);
        if (rc < 0) {
            if (errno == EINTR) continue;
            return 0;
        }
        if (rc == 0) continue;
        if (pfd.revents & (POLLIN | POLLHUP | POLLERR)) {
            int res = recv_packet(fd, pkt);
            return (res == 1) ? 1 : 0;
        }
    }
    return -1;
}

int main(int argc, char *argv[]) {
    if (argc < 6) {
        fd_printf(STDERR_FILENO,
                  "Использование: %s <ip> <port> <dept_id> <service_ms> <item1> [item2 ...]\n"
                  "Пример:        %s 127.0.0.1 8080 1 600 101 102\n",
                  argv[0], argv[0]);
        return 1;
    }

    const char *ip = argv[1];
    int port = atoi(argv[2]);
    int dept_id = atoi(argv[3]);
    int service_ms = atoi(argv[4]);
    if (service_ms <= 0) service_ms = 500;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    int fd = connect_tcp(ip, port);
    if (fd < 0) return 1;

    Packet reg;
    memset(&reg, 0, sizeof(reg));
    reg.type = MSG_REG_DEPT;
    reg.id = dept_id;
    reg.value = service_ms;
    int item_count = argc - 5;
    if (item_count > MAX_ITEMS) item_count = MAX_ITEMS;
    reg.count = item_count;
    for (int i = 0; i < item_count; i++) {
        reg.items[i] = atoi(argv[5 + i]);
    }

    if (send_packet(fd, &reg) < 0) {
        fd_printf(STDERR_FILENO, "[Отдел %d] Ошибка отправки регистрации.\n", dept_id);
        close(fd);
        return 1;
    }

    fd_printf(STDOUT_FILENO, "[Отдел %d] Подключен к %s:%d (обслуживание: %d мс, товаров: %d).\n",
              dept_id, ip, port, service_ms, item_count);

    while (running) {
        Packet pkt;
        int rc = recv_interruptible(fd, &pkt);
        if (rc == -1) {
            fd_printf(STDOUT_FILENO, "\n[Отдел %d] Получен сигнал прерывания. Отключение...\n", dept_id);
            break;
        }
        if (rc == 0) {
            fd_printf(STDOUT_FILENO, "[Отдел %d] Соединение с сервером закрыто.\n", dept_id);
            break;
        }

        if (pkt.type == MSG_SERVE_REQ) {
            fd_printf(STDOUT_FILENO, "[Отдел %d] Обслуживаю покупателя %d (товар %d)...\n",
                      dept_id, pkt.id, pkt.item_id);

            // Разбиваем задержку на шаги по 100 мс для быстрой реакции на Ctrl+C
            int waited = 0;
            while (waited < service_ms && running) {
                int step = (service_ms - waited > 100) ? 100 : (service_ms - waited);
                msleep(step);
                waited += step;
            }
            if (!running) {
                fd_printf(STDOUT_FILENO, "\n[Отдел %d] Прерван во время обслуживания. Выход.\n", dept_id);
                break;
            }

            Packet done;
            memset(&done, 0, sizeof(done));
            done.type = MSG_SERVE_DONE;
            done.id = pkt.id;
            done.item_id = pkt.item_id;
            done.dept_id = dept_id;
            if (send_packet(fd, &done) < 0) {
                fd_printf(STDERR_FILENO, "[Отдел %d] Потеряна связь с сервером при выдаче товара.\n", dept_id);
                break;
            }
            fd_printf(STDOUT_FILENO, "[Отдел %d] Выдал товар %d покупателю %d.\n",
                      dept_id, pkt.item_id, pkt.id);
        } else if (pkt.type == MSG_SHUTDOWN) {
            fd_printf(STDOUT_FILENO, "[Отдел %d] Сервер завершает работу. Штатный выход.\n", dept_id);
            break;
        } else if (pkt.type == MSG_ERROR) {
            fd_printf(STDERR_FILENO, "[Отдел %d] Ошибка сервера: %s", dept_id, pkt.text);
            break;
        }
    }

    close(fd);
    return 0;
}