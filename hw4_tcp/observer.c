#include "protocol.h"

static volatile sig_atomic_t running = 1;

static void handle_signal(int sig) {
    (void)sig;
    running = 0;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fd_printf(STDERR_FILENO,
                  "Использование: %s <ip> <port> [log_file]\n"
                  "Пример:        %s 127.0.0.1 8080\n",
                  argv[0], argv[0]);
        return 1;
    }

    const char *ip = argv[1];
    int port = atoi(argv[2]);
    int out_fd = -1;
    if (argc >= 4) {
        out_fd = open(argv[3], O_CREAT | O_WRONLY | O_TRUNC, 0644);
    }

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    int fd = connect_tcp(ip, port);
    if (fd < 0) {
        if (out_fd != -1) close(out_fd);
        return 1;
    }

    Packet reg;
    memset(&reg, 0, sizeof(reg));
    reg.type = MSG_REG_OBSERVER;
    if (send_packet(fd, &reg) < 0) {
        fd_printf(STDERR_FILENO, "[Наблюдатель] Не удалось зарегистрироваться на сервере.\n");
        close(fd);
        if (out_fd != -1) close(out_fd);
        return 1;
    }

    while (running) {
        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int rc = poll(&pfd, 1, 200);
        if (rc < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (rc == 0) continue;

        if (pfd.revents & (POLLIN | POLLHUP | POLLERR)) {
            Packet pkt;
            int res = recv_packet(fd, &pkt);
            if (res <= 0) {
                fd_printf(STDOUT_FILENO, "[Наблюдатель] Соединение с сервером закрыто.\n");
                break;
            }
            if (pkt.type == MSG_LOG) {
                write_all(STDOUT_FILENO, pkt.text, strlen(pkt.text));
                if (out_fd != -1) {
                    write_all(out_fd, pkt.text, strlen(pkt.text));
                }
            } else if (pkt.type == MSG_SHUTDOWN) {
                fd_printf(STDOUT_FILENO, "[Наблюдатель] Сервер завершил работу. Штатное отключение.\n");
                break;
            } else if (pkt.type == MSG_ERROR) {
                fd_printf(STDERR_FILENO, "[Наблюдатель] Ошибка сервера: %s", pkt.text);
                break;
            }
        }
    }

    if (!running) {
        fd_printf(STDOUT_FILENO, "\n[Наблюдатель] Отключен пользователем (SIGINT).\n");
    }

    close(fd);
    if (out_fd != -1) close(out_fd);
    return 0;
}