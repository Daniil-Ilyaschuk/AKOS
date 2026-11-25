#include "protocol.h"

// Элемент очереди покупателей внутри отдела
typedef struct {
    int cust_id;
    int item_id;
    bool in_queue;       // находится ли запись в очереди отдела
    bool in_service;     // обслуживается ли сейчас продавцом
    bool served;         // завершено ли обслуживание успешно
    bool customer_gone;  // отключился ли покупатель до завершения
    pthread_cond_t done_cond;
} QueueEntry;

// Состояние отдела магазина (сохраняется на сервере даже при переподключении продавца)
typedef struct {
    int id;
    int assortment[MAX_ITEMS];
    int assortment_size;
    int service_ms;
    bool online;
    bool is_sleeping;
    int fd;
    QueueEntry *queue[MAX_QUEUE];
    int queue_size;
    pthread_cond_t cond;
} Department;

// Глобальное состояние сервера
static Department depts[MAX_DEPTS];
static int num_depts = 0;

static int observer_fds[MAX_OBSERVERS];
static int num_observers = 0;

static pthread_mutex_t shop_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t obs_mutex  = PTHREAD_MUTEX_INITIALIZER;

static volatile sig_atomic_t server_running = 1;
static bool doors_closed = false;
static int active_customers = 0;
static int active_threads = 0;
static int log_fd = -1;

// Безопасный обработчик сигналов прерывания (async-signal-safe)
static void handle_signal(int sig) {
    (void)sig;
    server_running = 0;
}

// Абсолютный таймаут для pthread_cond_timedwait
static void get_abs_timeout(struct timespec *ts, int ms) {
    clock_gettime(CLOCK_REALTIME, ts);
    ts->tv_sec += ms / 1000;
    ts->tv_nsec += (long)(ms % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec += 1;
        ts->tv_nsec -= 1000000000L;
    }
}

// Проверка, не разорвал ли удаленный клиент TCP-соединение (неблокирующая)
static bool is_peer_disconnected(int fd) {
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    int rc = poll(&pfd, 1, 0);
    if (rc > 0 && (pfd.revents & (POLLIN | POLLHUP | POLLERR))) {
        char tmp;
        ssize_t n = recv(fd, &tmp, 1, MSG_PEEK | MSG_DONTWAIT);
        if (n == 0) return true;
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) return true;
    }
    return false;
}

// Ожидание пакета с периодической проверкой флага server_running
// Возвращает: 1 — пакет получен, 0 — разрыв соединения, -1 — сервер останавливается
static int recv_packet_interruptible(int fd, Packet *pkt) {
    while (server_running) {
        struct pollfd pfd = { .fd = fd, .events = POLLIN };
        int rc = poll(&pfd, 1, 200);
        if (rc < 0) {
            if (errno == EINTR) continue;
            return 0;
        }
        if (rc == 0) continue; // таймаут — проверяем server_running
        if (pfd.revents & (POLLIN | POLLHUP | POLLERR)) {
            int res = recv_packet(fd, pkt);
            return (res == 1) ? 1 : 0;
        }
    }
    return -1;
}

// Рассылка события всем подключенным наблюдателям и запись в файл sim_log.txt.
// При print_on_server == true событие также выводится в консоль самого сервера.
static void broadcast_event(bool print_on_server, const char *fmt, ...) {
    Packet pkt;
    memset(&pkt, 0, sizeof(pkt));
    pkt.type = MSG_LOG;

    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(pkt.text, sizeof(pkt.text), fmt, args);
    va_end(args);
    if (len <= 0) return;

    pthread_mutex_lock(&obs_mutex);
    if (print_on_server) {
        write_all(STDOUT_FILENO, pkt.text, strlen(pkt.text));
    }
    if (log_fd != -1) {
        write_all(log_fd, pkt.text, strlen(pkt.text));
    }
    for (int i = 0; i < num_observers; i++) {
        if (observer_fds[i] >= 0) {
            send_packet(observer_fds[i], &pkt);
        }
    }
    pthread_mutex_unlock(&obs_mutex);
}

// --- Операции с очередью отдела (вызываются под shop_mutex) ---

static bool enqueue_back(Department *d, QueueEntry *e) {
    if (d->queue_size >= MAX_QUEUE) return false;
    d->queue[d->queue_size++] = e;
    e->in_queue = true;
    return true;
}

static void enqueue_front(Department *d, QueueEntry *e) {
    if (d->queue_size >= MAX_QUEUE) return;
    for (int i = d->queue_size; i > 0; i--) {
        d->queue[i] = d->queue[i - 1];
    }
    d->queue[0] = e;
    d->queue_size++;
    e->in_queue = true;
}

static QueueEntry *dequeue_front(Department *d) {
    if (d->queue_size == 0) return NULL;
    QueueEntry *e = d->queue[0];
    for (int i = 1; i < d->queue_size; i++) {
        d->queue[i - 1] = d->queue[i];
    }
    d->queue_size--;
    e->in_queue = false;
    return e;
}

static void remove_entry(Department *d, QueueEntry *e) {
    for (int i = 0; i < d->queue_size; i++) {
        if (d->queue[i] == e) {
            for (int j = i + 1; j < d->queue_size; j++) {
                d->queue[j - 1] = d->queue[j];
            }
            d->queue_size--;
            e->in_queue = false;
            return;
        }
    }
}

static Department *find_dept_by_id(int dept_id) {
    for (int i = 0; i < num_depts; i++) {
        if (depts[i].id == dept_id) return &depts[i];
    }
    return NULL;
}

static Department *find_dept_for_item(int item_id) {
    for (int i = 0; i < num_depts; i++) {
        for (int j = 0; j < depts[i].assortment_size; j++) {
            if (depts[i].assortment[j] == item_id) return &depts[i];
        }
    }
    return NULL;
}

// --- Обработчик клиента-наблюдателя ---
static void handle_observer(int fd) {
    pthread_mutex_lock(&obs_mutex);
    if (num_observers >= MAX_OBSERVERS) {
        pthread_mutex_unlock(&obs_mutex);
        Packet err = { .type = MSG_ERROR };
        snprintf(err.text, MAX_TEXT, "Достигнут лимит наблюдателей.\n");
        send_packet(fd, &err);
        return;
    }
    Packet welcome;
    memset(&welcome, 0, sizeof(welcome));
    welcome.type = MSG_LOG;
    snprintf(welcome.text, MAX_TEXT, ">>> Подключение к серверу магазина установлено. Ожидание событий...\n");
    if (send_packet(fd, &welcome) < 0) {
        pthread_mutex_unlock(&obs_mutex);
        return;
    }
    observer_fds[num_observers++] = fd;
    int obs_count = num_observers;
    pthread_mutex_unlock(&obs_mutex);

    fd_printf(STDOUT_FILENO, "[Сервер] Подключился наблюдатель (активных: %d).\n", obs_count);

    // Ждем отключения наблюдателя или завершения работы сервера
    while (server_running) {
        if (is_peer_disconnected(fd)) {
            break;
        }
        msleep(200);
    }

    // Удаляем дескриптор из списка рассылки
    pthread_mutex_lock(&obs_mutex);
    for (int i = 0; i < num_observers; i++) {
        if (observer_fds[i] == fd) {
            observer_fds[i] = observer_fds[num_observers - 1];
            num_observers--;
            break;
        }
    }
    obs_count = num_observers;
    pthread_mutex_unlock(&obs_mutex);

    if (!server_running) {
        Packet bye = { .type = MSG_SHUTDOWN };
        send_packet(fd, &bye);
    }
    fd_printf(STDOUT_FILENO, "[Сервер] Наблюдатель отключился (активных: %d).\n", obs_count);
}

// --- Обработчик клиента-отдела (продавца) ---
static void handle_department(int fd, const Packet *reg) {
    pthread_mutex_lock(&shop_mutex);
    Department *d = find_dept_by_id(reg->id);
    bool reconnected = (d != NULL);

    if (d != NULL && d->online) {
        pthread_mutex_unlock(&shop_mutex);
        Packet err = { .type = MSG_ERROR };
        snprintf(err.text, MAX_TEXT, "Отдел %d уже подключен к серверу.\n", reg->id);
        send_packet(fd, &err);
        return;
    }

    if (d == NULL) {
        if (num_depts >= MAX_DEPTS) {
            pthread_mutex_unlock(&shop_mutex);
            Packet err = { .type = MSG_ERROR };
            snprintf(err.text, MAX_TEXT, "Превышен лимит отделов.\n");
            send_packet(fd, &err);
            return;
        }
        d = &depts[num_depts++];
        memset(d, 0, sizeof(Department));
        d->id = reg->id;
        pthread_cond_init(&d->cond, NULL);
    }

    d->assortment_size = (reg->count > MAX_ITEMS) ? MAX_ITEMS : reg->count;
    for (int i = 0; i < d->assortment_size; i++) {
        d->assortment[i] = reg->items[i];
    }
    d->service_ms = (reg->value > 0) ? reg->value : 500;
    d->online = true;
    d->fd = fd;
    d->is_sleeping = (d->queue_size == 0);
    int pending = d->queue_size;
    pthread_mutex_unlock(&shop_mutex);

    if (reconnected) {
        broadcast_event(true, "[Отдел %d] Продавец вернулся на рабочее место (в очереди: %d).\n", d->id, pending);
    } else {
        broadcast_event(true, "[Отдел %d] Открыт и готов к работе (товаров в ассортименте: %d).\n", d->id, d->assortment_size);
    }

    while (server_running) {
        pthread_mutex_lock(&shop_mutex);
        while (d->queue_size == 0 && server_running) {
            if (!d->is_sleeping) {
                d->is_sleeping = true;
                pthread_mutex_unlock(&shop_mutex);
                broadcast_event(false, "Продавец отдела %d засыпает из-за отсутствия покупателей.\n", d->id);
                pthread_mutex_lock(&shop_mutex);
                if (d->queue_size > 0 || !server_running) break;
            }

            // Проверяем, не отключился ли продавец, пока спит
            if (is_peer_disconnected(fd)) {
                d->online = false;
                d->fd = -1;
                pthread_mutex_unlock(&shop_mutex);
                broadcast_event(true, "[Отдел %d] Продавец отключился (ожидаем переподключения).\n", d->id);
                return;
            }

            struct timespec ts;
            get_abs_timeout(&ts, 200);
            pthread_cond_timedwait(&d->cond, &shop_mutex, &ts);
        }

        if (!server_running) {
            d->online = false;
            d->fd = -1;
            pthread_mutex_unlock(&shop_mutex);
            Packet bye = { .type = MSG_SHUTDOWN };
            send_packet(fd, &bye);
            return;
        }

        QueueEntry *entry = dequeue_front(d);
        if (!entry) {
            pthread_mutex_unlock(&shop_mutex);
            continue;
        }
        entry->in_service = true;
        int cust_id = entry->cust_id;
        int item_id = entry->item_id;
        pthread_mutex_unlock(&shop_mutex);

        broadcast_event(false, "Продавец отдела %d обслуживает покупателя %d (товар %d).\n",
                        d->id, cust_id, item_id);

        Packet req = { .type = MSG_SERVE_REQ, .id = cust_id, .item_id = item_id, .dept_id = d->id };
        if (send_packet(fd, &req) < 0) {
            // Продавец отвалился в момент отправки задания — возвращаем покупателя в начало очереди
            pthread_mutex_lock(&shop_mutex);
            entry->in_service = false;
            d->online = false;
            d->fd = -1;
            if (entry->customer_gone) {
                pthread_cond_destroy(&entry->done_cond);
                free(entry);
            } else {
                enqueue_front(d, entry);
            }
            pthread_mutex_unlock(&shop_mutex);
            broadcast_event(true, "[Отдел %d] Связь с продавцом потеряна! Покупатель %d ждет возвращения продавца.\n",
                            d->id, cust_id);
            return;
        }

        Packet resp;
        int rc = recv_packet_interruptible(fd, &resp);
        if (rc == -1) {
            // Сервер завершает работу
            pthread_mutex_lock(&shop_mutex);
            entry->in_service = false;
            d->online = false;
            d->fd = -1;
            if (entry->customer_gone) {
                pthread_cond_destroy(&entry->done_cond);
                free(entry);
            } else {
                pthread_cond_signal(&entry->done_cond);
            }
            pthread_mutex_unlock(&shop_mutex);
            Packet bye = { .type = MSG_SHUTDOWN };
            send_packet(fd, &bye);
            return;
        }
        if (rc == 0 || resp.type != MSG_SERVE_DONE) {
            // Продавец отключился прямо во время обслуживания
            pthread_mutex_lock(&shop_mutex);
            entry->in_service = false;
            d->online = false;
            d->fd = -1;
            if (entry->customer_gone) {
                pthread_cond_destroy(&entry->done_cond);
                free(entry);
            } else {
                enqueue_front(d, entry);
            }
            pthread_mutex_unlock(&shop_mutex);
            broadcast_event(true, "[Отдел %d] Продавец отключился во время обслуживания! Покупатель %d возвращен в начало очереди.\n",
                            d->id, cust_id);
            return;
        }

        // Обслуживание успешно завершено
        bool gone = false;
        pthread_mutex_lock(&shop_mutex);
        entry->in_service = false;
        if (entry->customer_gone) {
            gone = true;
            pthread_cond_destroy(&entry->done_cond);
            free(entry);
        } else {
            entry->served = true;
            pthread_cond_signal(&entry->done_cond);
        }
        pthread_mutex_unlock(&shop_mutex);

        if (gone) {
            broadcast_event(false, "Продавец отдела %d закончил, но покупатель %d уже покинул магазин.\n",
                            d->id, cust_id);
        } else {
            broadcast_event(false, "Покупатель %d покупает товар %d в отделе %d.\n",
                            cust_id, item_id, d->id);
        }
    }

    pthread_mutex_lock(&shop_mutex);
    d->online = false;
    d->fd = -1;
    pthread_mutex_unlock(&shop_mutex);
    Packet bye = { .type = MSG_SHUTDOWN };
    send_packet(fd, &bye);
}

// --- Обработчик клиента-покупателя ---
static void handle_customer(int fd, const Packet *reg) {
    int cust_id = reg->id;

    pthread_mutex_lock(&shop_mutex);
    if (doors_closed || !server_running) {
        pthread_mutex_unlock(&shop_mutex);
        Packet closed;
        memset(&closed, 0, sizeof(closed));
        closed.type = MSG_CLOSED;
        send_packet(fd, &closed);
        broadcast_event(true, "Покупатель %d пришел после закрытия дверей и разворачивается.\n", cust_id);
        return;
    }
    active_customers++;
    pthread_mutex_unlock(&shop_mutex);

    Packet reg_ok;
    memset(&reg_ok, 0, sizeof(reg_ok));
    reg_ok.type = MSG_REG_OK;
    if (send_packet(fd, &reg_ok) < 0) {
        pthread_mutex_lock(&shop_mutex);
        active_customers--;
        pthread_mutex_unlock(&shop_mutex);
        return;
    }

    char list_str[128] = "[";
    size_t pos = 1;
    int item_cnt = (reg->count < 0) ? 0 : ((reg->count > MAX_ITEMS) ? MAX_ITEMS : reg->count);
    for (int i = 0; i < item_cnt; i++) {
        int n = snprintf(list_str + pos, sizeof(list_str) - pos, "%s%d",
                         (i > 0) ? ", " : "", reg->items[i]);
        if (n < 0 || pos + (size_t)n >= sizeof(list_str) - 2) break;
        pos += (size_t)n;
    }
    list_str[pos++] = ']';
    list_str[pos] = '\0';

    broadcast_event(true, "Покупатель %d вошел в магазин. Список покупок: %s.\n", cust_id, list_str);

    while (server_running) {
        Packet req;
        int rc = recv_packet_interruptible(fd, &req);
        if (rc == -1) {
            Packet bye;
            memset(&bye, 0, sizeof(bye));
            bye.type = MSG_SHUTDOWN;
            send_packet(fd, &bye);
            break;
        }
        if (rc == 0) {
            broadcast_event(true, "Покупатель %d неожиданно покинул магазин (разрыв связи).\n", cust_id);
            break;
        }
        if (req.type == MSG_CUST_LEAVE) {
            broadcast_event(true, "Покупатель %d завершил список покупок и покинул магазин.\n", cust_id);
            break;
        }
        if (req.type != MSG_BUY_REQ) {
            continue;
        }

        int item_id = req.item_id;
        pthread_mutex_lock(&shop_mutex);
        Department *d = find_dept_for_item(item_id);
        if (d == NULL) {
            pthread_mutex_unlock(&shop_mutex);
            Packet nf;
            memset(&nf, 0, sizeof(nf));
            nf.type = MSG_ITEM_NOT_FOUND;
            nf.item_id = item_id;
            send_packet(fd, &nf);
            broadcast_event(false, "Покупатель %d не нашел товар %d в магазине.\n", cust_id, item_id);
            continue;
        }

        QueueEntry *entry = (QueueEntry *)calloc(1, sizeof(QueueEntry));
        entry->cust_id = cust_id;
        entry->item_id = item_id;
        pthread_cond_init(&entry->done_cond, NULL);

        if (!enqueue_back(d, entry)) {
            pthread_cond_destroy(&entry->done_cond);
            free(entry);
            pthread_mutex_unlock(&shop_mutex);
            Packet err;
            memset(&err, 0, sizeof(err));
            err.type = MSG_ERROR;
            snprintf(err.text, MAX_TEXT, "Очередь в отдел %d переполнена.\n", d->id);
            send_packet(fd, &err);
            continue;
        }

        int dept_id = d->id;
        int qsize = d->queue_size;
        bool was_sleeping = d->is_sleeping;
        bool dept_online = d->online;
        if (dept_online && was_sleeping) {
            d->is_sleeping = false;
        }
        if (dept_online) {
            pthread_cond_signal(&d->cond);
        }
        pthread_mutex_unlock(&shop_mutex);

        broadcast_event(false, "Покупатель %d направляется в отдел %d за товаром %d и встает в очередь (в очереди: %d).\n",
                        cust_id, dept_id, item_id, qsize);
        if (dept_online && was_sleeping) {
            broadcast_event(false, "Продавец отдела %d просыпается!\n", dept_id);
        } else if (!dept_online) {
            broadcast_event(false, "Отдел %d временно без продавца — покупатель %d ждет его возвращения.\n",
                            dept_id, cust_id);
        }

        // Ожидаем обслуживания с контролем отключения покупателя и остановки сервера
        bool disconnected = false;
        pthread_mutex_lock(&shop_mutex);
        while (!entry->served && server_running) {
            if (is_peer_disconnected(fd)) {
                disconnected = true;
                break;
            }
            struct timespec ts;
            get_abs_timeout(&ts, 200);
            pthread_cond_timedwait(&entry->done_cond, &shop_mutex, &ts);
        }

        bool served = entry->served;
        if (entry->in_queue) {
            remove_entry(d, entry);
        }
        if (entry->in_service) {
            // Запись еще держит поток продавца - он освободит память сам
            entry->customer_gone = true;
        } else {
            pthread_cond_destroy(&entry->done_cond);
            free(entry);
        }
        pthread_mutex_unlock(&shop_mutex);

        if (disconnected) {
            broadcast_event(true, "Покупатель %d вышел из очереди отдела %d (разрыв связи).\n", cust_id, dept_id);
            break;
        }
        if (!server_running) {
            Packet bye;
            memset(&bye, 0, sizeof(bye));
            bye.type = MSG_SHUTDOWN;
            send_packet(fd, &bye);
            break;
        }
        if (served) {
            Packet ok;
            memset(&ok, 0, sizeof(ok));
            ok.type = MSG_BUY_OK;
            ok.item_id = item_id;
            ok.dept_id = dept_id;
            if (send_packet(fd, &ok) < 0) {
                broadcast_event(true, "Покупатель %d отключился до получения товара %d.\n", cust_id, item_id);
                break;
            }
        }
    }

    pthread_mutex_lock(&shop_mutex);
    active_customers--;
    pthread_mutex_unlock(&shop_mutex);
}

// Поток первичного разбора входящего TCP-подключения
static void *client_worker(void *arg) {
    int fd = *(int *)arg;
    free(arg);

    Packet reg;
    int rc = recv_packet_interruptible(fd, &reg);
    if (rc == 1) {
        if (reg.type == MSG_REG_OBSERVER) {
            handle_observer(fd);
        } else if (reg.type == MSG_REG_DEPT) {
            handle_department(fd, &reg);
        } else if (reg.type == MSG_REG_CUST) {
            handle_customer(fd, &reg);
        }
    }

    close(fd);
    pthread_mutex_lock(&shop_mutex);
    active_threads--;
    pthread_mutex_unlock(&shop_mutex);
    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fd_printf(STDERR_FILENO, "Использование: %s <ip> <port> [workday_sec]\n", argv[0]);
        fd_printf(STDERR_FILENO, "Пример:        %s 0.0.0.0 8080 0   (0 = работа до Ctrl+C)\n", argv[0]);
        return 1;
    }

    const char *bind_ip = argv[1];
    int port = atoi(argv[2]);
    int workday_sec = (argc >= 4) ? atoi(argv[3]) : 0;

    // Игнорируем SIGPIPE, чтобы разрыв соединения с клиентом не ронял сервер
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    log_fd = open("sim_log.txt", O_CREAT | O_WRONLY | O_TRUNC, 0644);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, bind_ip, &addr.sin_addr) <= 0) {
        fd_printf(STDERR_FILENO, "Ошибка: некорректный IP-адрес '%s'\n", bind_ip);
        close(listen_fd);
        return 1;
    }

    if (bind(listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, 32) < 0) {
        perror("listen");
        close(listen_fd);
        return 1;
    }

    time_t start_time = time(NULL);
    broadcast_event(true, "=== Сервер магазина запущен на %s:%d (лимит дня: %s) ===\n",
                    bind_ip, port, (workday_sec > 0) ? "по таймеру" : "до Ctrl+C");

    // Основной цикл приема подключений
    while (server_running) {
        if (workday_sec > 0) {
            time_t elapsed = time(NULL) - start_time;
            if (!doors_closed && elapsed >= workday_sec) {
                pthread_mutex_lock(&shop_mutex);
                doors_closed = true;
                pthread_mutex_unlock(&shop_mutex);
                broadcast_event(true, ">>> Время вышло: магазин закрывает двери на вход!\n");
            }
            if (doors_closed) {
                pthread_mutex_lock(&shop_mutex);
                int left = active_customers;
                pthread_mutex_unlock(&shop_mutex);
                if (left == 0) {
                    broadcast_event(true, "Все покупатели обслужены. Магазин закрыт.\n");
                    server_running = 0;
                    break;
                }
            }
        }

        struct pollfd pfd = { .fd = listen_fd, .events = POLLIN };
        int rc = poll(&pfd, 1, 200);
        if (rc < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (rc == 0) continue;

        if (pfd.revents & POLLIN) {
            int client_fd = accept(listen_fd, NULL, NULL);
            if (client_fd < 0) continue;

            int *fd_ptr = (int *)malloc(sizeof(int));
            *fd_ptr = client_fd;

            pthread_mutex_lock(&shop_mutex);
            active_threads++;
            pthread_mutex_unlock(&shop_mutex);

            pthread_t tid;
            pthread_attr_t attr;
            pthread_attr_init(&attr);
            pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
            if (pthread_create(&tid, &attr, client_worker, fd_ptr) != 0) {
                free(fd_ptr);
                close(client_fd);
                pthread_mutex_lock(&shop_mutex);
                active_threads--;
                pthread_mutex_unlock(&shop_mutex);
            }
            pthread_attr_destroy(&attr);
        }
    }

    // Оповещаем все внутренние потоки о завершении (они разошлют MSG_SHUTDOWN клиентам)
    broadcast_event(true, ">>> Сервер завершает работу и отключает всех клиентов...\n");
    pthread_mutex_lock(&shop_mutex);
    for (int i = 0; i < num_depts; i++) {
        pthread_cond_broadcast(&depts[i].cond);
    }
    pthread_mutex_unlock(&shop_mutex);

    // Ждем корректного завершения клиентских потоков (до 2 секунд)
    for (int i = 0; i < 40; i++) {
        pthread_mutex_lock(&shop_mutex);
        int left = active_threads;
        pthread_mutex_unlock(&shop_mutex);
        if (left == 0) break;
        msleep(50);
    }

    close(listen_fd);
    if (log_fd != -1) close(log_fd);
    fd_printf(STDOUT_FILENO, "=== Сервер корректно остановлен. ===\n");
    return 0;
}