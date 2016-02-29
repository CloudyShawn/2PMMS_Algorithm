CC = gcc
CFLAGS = -Wall -Werror

all: disk_sort

disk_sort: disk_sort.c
	$(CC) -o $@ $^ $(CFLAGS)
clean:
	rm -f disk_sort run_* reverse_edges.dat
