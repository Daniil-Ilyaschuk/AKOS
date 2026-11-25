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
    if (argc < 5) {
        fd_printf(STDERR_FILENO,
                  "Использование: %s <ip> <port> <cust_id> <item1> [item2 ...]\n"
                  "Пример:        %s 127.0.0.1 8080 1 101 202 102\n",
                  argv[0], argv[0]);
        return 1;
    }

    const char *ip = argv[1];
    int port = atoi(argv[2]);
    int cust_id = atoi(argv[3]);
    int list_size = argc - 4;
    if (list_size > MAX_ITEMS) list_size = MAX_ITEMS;

    int items[MAX_ITEMS];
    for (int i = 0; i < list_size; i++) {
        items[i] = atoi(argv[4 + i]);
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    int fd = connect_tcp(ip, port);
    if (fd < 0) return 1;

    Packet reg;
    memset(&reg, 0, sizeof(reg));
    reg.type = MSG_REG_CUST;
    reg.id = cust_id;
    reg.count = list_size;
    for (int i = 0; i < list_size; i++) {
        reg.items[i] = items[i];
    }
    if (send_packet(fd, &reg) < 0) {
        fd_printf(STDERR_FILENO, "[Покупатель %d] Ошибка подключения к магазину.\n", cust_id);
        close(fd);
        return 1;
    }

    // Ждем подтверждения регистрации до отправки запросов на покупку
    Packet reg_resp;
    int reg_rc = recv_interruptible(fd, &reg_resp);
    if (reg_rc <= 0 || reg_resp.type != MSG_REG_OK) {
        if (reg_rc == 1 && reg_resp.type == MSG_CLOSED) {
            fd_printf(STDOUT_FILENO, "[Покупатель %d] Магазин закрыт на вход, разворачиваюсь.\n", cust_id);
        } else if (reg_rc == 1 && reg_resp.type == MSG_SHUTDOWN) {
            fd_printf(STDOUT_FILENO, "[Покупатель %d] Сервер завершает работу. Штатный выход.\n", cust_id);
        } else if (reg_rc == 1 && reg_resp.type == MSG_ERROR) {
            fd_printf(STDERR_FILENO, "[Покупатель %d] Ошибка сервера: %s", cust_id, reg_resp.text);
        } else {
            fd_printf(STDERR_FILENO, "[Покупатель %d] Не удалось войти в магазин.\n", cust_id);
        }
        close(fd);
        return 0;
    }

    char list_str[128] = "[";
    size_t pos = 1;
    for (int i = 0; i < list_size; i++) {
        int n = snprintf(list_str + pos, sizeof(list_str) - pos, "%s%d",
                         (i > 0) ? ", " : "", items[i]);
        if (n < 0 || pos + (size_t)n >= sizeof(list_str) - 2) break;
        pos += (size_t)n;
    }
    list_str[pos++] = ']';
    list_str[pos] = '\0';

    fd_printf(STDOUT_FILENO, "[Покупатель %d] Зашел в магазин (%s:%d). Список покупок: %s.\n",
              cust_id, ip, port, list_str);

    bool completed = true;
    for (int i = 0; i < list_size && running; i++) {
        int item_id = items[i];
        fd_printf(STDOUT_FILENO, "[Покупатель %d] Запрашиваю товар %d...\n", cust_id, item_id);

        Packet req;
        memset(&req, 0, sizeof(req));
        req.type = MSG_BUY_REQ;
        req.id = cust_id;
        req.item_id = item_id;
        if (send_packet(fd, &req) < 0) {
            fd_printf(STDERR_FILENO, "[Покупатель %d] Сервер разорвал соединение.\n", cust_id);
            completed = false;
            break;
        }

        Packet resp;
        int rc = recv_interruptible(fd, &resp);
        if (rc == -1) {
            fd_printf(STDOUT_FILENO, "\n[Покупатель %d] Получен сигнал прерывания. Покидаю очередь.\n", cust_id);
            completed = false;
            break;
        }
        if (rc == 0) {
            fd_printf(STDOUT_FILENO, "[Покупатель %d] Соединение с сервером закрыто.\n", cust_id);
            completed = false;
            break;
        }

        if (resp.type == MSG_BUY_OK) {
            fd_printf(STDOUT_FILENO, "[Покупатель %d] Купил товар %d в отделе %d.\n",
                      cust_id, resp.item_id, resp.dept_id);
            msleep(200);
        } else if (resp.type == MSG_ITEM_NOT_FOUND) {
            fd_printf(STDOUT_FILENO, "[Покупатель %d] Товар %d не найден, пропускаю.\n",
                      cust_id, resp.item_id);
        } else if (resp.type == MSG_CLOSED) {
            fd_printf(STDOUT_FILENO, "[Покупатель %d] Магазин закрыт на вход, разворачиваюсь.\n", cust_id);
            completed = false;
            break;
        } else if (resp.type == MSG_SHUTDOWN) {
            fd_printf(STDOUT_FILENO, "[Покупатель %d] Сервер завершает работу. Штатный выход.\n", cust_id);
            completed = false;
            break;
        } else if (resp.type == MSG_ERROR) {
            fd_printf(STDERR_FILENO, "[Покупатель %d] Ошибка сервера: %s", cust_id, resp.text);
            completed = false;
            break;
        }
    }

    if (completed && running) {
        Packet leave;
        memset(&leave, 0, sizeof(leave));
        leave.type = MSG_CUST_LEAVE;
        leave.id = cust_id;
        send_packet(fd, &leave);
        fd_printf(STDOUT_FILENO, "[Покупатель %d] Список завершен, покидаю магазин.\n", cust_id);
    }

    close(fd);
    return 0;
}