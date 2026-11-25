#ifndef PROTOCOL_H
#define PROTOCOL_H

#define _POSIX_C_SOURCE 200809L
#ifdef __APPLE__
#define _DARWIN_C_SOURCE
#endif

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#define MAX_DEPTS     16
#define MAX_ITEMS     20
#define MAX_QUEUE     64
#define MAX_OBSERVERS 32
#define MAX_TEXT      256

// Типы сообщений протокола прикладного уровня поверх TCP
typedef enum {
    MSG_REG_DEPT       = 1,  // Регистрация отдела (id, value=service_ms, count, items[])
    MSG_REG_CUST       = 2,  // // Регистрация покупателя (id, count, items[])
    MSG_REG_OBSERVER   = 3,  // Регистрация клиента-наблюдателя
    MSG_BUY_REQ        = 4,  // Запрос покупателя на покупку товара (id=cust_id, item_id)
    MSG_BUY_OK         = 5,  // Подтверждение покупки (item_id, dept_id)
    MSG_ITEM_NOT_FOUND = 6,  // Товар отсутствует в ассортименте магазина (item_id)
    MSG_SERVE_REQ      = 7,  // Задание сервер -> отдел: обслужить покупателя (id=cust_id, item_id)
    MSG_SERVE_DONE     = 8,  // Ответ отдел -> сервер: покупатель обслужен (id=cust_id, item_id)
    MSG_CUST_LEAVE     = 9,  // Покупатель завершил покупки и уходит (id=cust_id)
    MSG_LOG            = 10, // Событие симуляции сервер -> наблюдатель (text)
    MSG_CLOSED         = 11, // Магазин закрыт на вход
    MSG_SHUTDOWN       = 12, // Сервер завершает работу — клиентам предписано завершиться
    MSG_ERROR          = 13,  // Ошибка регистрации/операции (text)
    MSG_REG_OK         = 14  // Подтверждение успешной регистрации клиента
} MsgType;

// Фиксированный пакет обмена по TCP (гарантирует четкие границы сообщений)
typedef struct {
    int32_t type;
    int32_t id;
    int32_t item_id;
    int32_t dept_id;
    int32_t value;
    int32_t count;
    int32_t items[MAX_ITEMS];
    char text[MAX_TEXT];
} Packet;

// Кроссплатформенная задержка в миллисекундах (POSIX.1-2008)
static inline void msleep(int ms) {
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
        // продолжаем сон, если прервано сигналом
    }
}

// Надежная запись буфера в файловый дескриптор через системный вызов write()
static inline int write_all(int fd, const void *buf, size_t len) {
    const char *ptr = (const char *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = write(fd, ptr + sent, len - sent);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        sent += (size_t)n;
    }
    return 0;
}

// Вывод форматированного сообщения в дескриптор (STDOUT_FILENO / файл лога)
static inline void fd_printf(int fd, const char *fmt, ...) {
    char buf[512];
    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    if (len > 0) {
        size_t to_write = (size_t)len < sizeof(buf) ? (size_t)len : sizeof(buf) - 1;
        write_all(fd, buf, to_write);
    }
}

// Надежная отправка полного пакета по TCP с переводом чисел в сетевой порядок байтов
static inline int send_packet(int sock_fd, const Packet *pkt) {
    Packet net = *pkt;
    net.type    = (int32_t)htonl((uint32_t)pkt->type);
    net.id      = (int32_t)htonl((uint32_t)pkt->id);
    net.item_id = (int32_t)htonl((uint32_t)pkt->item_id);
    net.dept_id = (int32_t)htonl((uint32_t)pkt->dept_id);
    net.value   = (int32_t)htonl((uint32_t)pkt->value);
    net.count   = (int32_t)htonl((uint32_t)pkt->count);
    for (int i = 0; i < MAX_ITEMS; i++) {
        net.items[i] = (int32_t)htonl((uint32_t)pkt->items[i]);
    }

    const char *ptr = (const char *)&net;
    size_t total = 0;
    while (total < sizeof(Packet)) {
        ssize_t n = send(sock_fd, ptr + total, sizeof(Packet) - total, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            return -1;
        }
        total += (size_t)n;
    }
    return 0;
}

// Надежное чтение полного пакета из TCP-потока.
// Возвращает: 1 — пакет принят, 0 — соединение закрыто (EOF), -1 — ошибка
static inline int recv_packet(int sock_fd, Packet *pkt) {
    Packet net;
    char *ptr = (char *)&net;
    size_t total = 0;
    while (total < sizeof(Packet)) {
        ssize_t n = recv(sock_fd, ptr + total, sizeof(Packet) - total, 0);
        if (n == 0) return 0;
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        total += (size_t)n;
    }

    pkt->type    = (int32_t)ntohl((uint32_t)net.type);
    pkt->id      = (int32_t)ntohl((uint32_t)net.id);
    pkt->item_id = (int32_t)ntohl((uint32_t)net.item_id);
    pkt->dept_id = (int32_t)ntohl((uint32_t)net.dept_id);
    pkt->value   = (int32_t)ntohl((uint32_t)net.value);
    pkt->count   = (int32_t)ntohl((uint32_t)net.count);
    for (int i = 0; i < MAX_ITEMS; i++) {
        pkt->items[i] = (int32_t)ntohl((uint32_t)net.items[i]);
    }
    memcpy(pkt->text, net.text, MAX_TEXT);
    pkt->text[MAX_TEXT - 1] = '\0';
    return 1;
}

// Вспомогательное подключение TCP-клиента к указанному IP и порту
static inline int connect_tcp(const char *ip, int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) <= 0) {
        fd_printf(STDERR_FILENO, "Ошибка: некорректный IP-адрес '%s'\n", ip);
        close(fd);
        return -1;
    }

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

#endif // PROTOCOL_H