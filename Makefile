CC = gcc
CFLAGS = -Wall -Werror

all: disk_sort

disk_sort: disk_sort.c
	$(CC) -o $@ $^ $(CFLAGS)
clean:
	rm disk_sort
