CC = gcc
CFLAGS = -Wall -Wextra -O2 -pthread

all: server department customer observer

server: server.c protocol.h
	$(CC) $(CFLAGS) server.c -o server

department: department.c protocol.h
	$(CC) $(CFLAGS) department.c -o department

customer: customer.c protocol.h
	$(CC) $(CFLAGS) customer.c -o customer

observer: observer.c protocol.h
	$(CC) $(CFLAGS) observer.c -o observer

clean:
	rm -f server department customer observer sim_log.txt obs_*.log