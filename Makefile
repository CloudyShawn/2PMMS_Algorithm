CC = gcc
CFLAGS = -Wall -Werror

all: disk_sort max_ave_followers

disk_sort: disk_sort.c
	$(CC) -o $@ $^ $(CFLAGS)

max_ave_followers: max_ave_followers.c
	$(CC) -o $@ $^ $(CFLAGS)

test: file_reader.c
	$(CC) -o file_reader $^ $(CFLAGS)

clean:
	rm -f disk_sort run_* reverse_edges.dat file_reader max_ave_followers
