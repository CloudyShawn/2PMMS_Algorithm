CC = gcc
CFLAGS = -Wall

all: disk_sort

disk_sort: disk_sort.c
	$(CC) -o $@ $^ $(CFLAGS)
clean:
	rm disk_sort *_run
