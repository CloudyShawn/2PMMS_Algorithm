/*
	* Arguments:
	* 1 - an array to sort
	* 2 - size of an array
	* 3 - size of each array element
	* 4 - function to compare two elements of the array

	qsort (buffer, total_records, sizeof(Record), compare);
*******************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "Merge.h"

int main(int argc, char *argv[])
{
  if(argc != 5)
  {
    printf("Usage: disk_sort <name of the input file> <total mem in bytes> <block size> <number of runs>\n");
    return 0;
  }

  /* Set up variables */
  int i;
  char *input_filename = argv[1];
  int total_mem = atoi(argv[2]);
  int block_size = atoi(argv[3]);
  int num_runs = atoi(argv[4]);

  /* Read in input file */
  FILE *in_file;
  FILE *out_file;
  if(!(in_file = fopen(input_filename, "rb")))
  {
    printf("Could not open file %s for reading\n", input_filename);
    return -1;
  }

  /* Calculate chunk size and round to block size */
  int chunk_size = total_mem / (num_runs);
  if(chunk_size % block_size > 0)
  {
    chunk_size -= chunk_size % block_size;
  }
  /* Calculate records in each chunk (used for buffer and runs) */
  int records_per_chunk = chunk_size / sizeof(Record);
  if(records_per_chunk == 0)
  {
    printf("Too many runs or block size too large\n");
    return (-1);
  }

  Record *in_buffer = (Record *)(calloc(records_per_chunk, sizeof(Record)));
  Record *out_buffer = (Record *)(calloc(records_per_chunk, sizeof(Record)));

  char i_string[10];
  int read_records = fread(in_buffer, sizeof(Record), records_per_chunk, in_file);
  for(i = 0; i < num_runs; i++)
  {
    sprintf(i_string, "%d", i);
    qsort(in_buffer, records_per_chunk, sizeof(Record), compare);

    out_file = fopen(strcat(i_string, OUTPUT_FILE_PREFIX), "wb");
    fwrite(in_buffer, sizeof(Record), read_records, out_file);

    read_records = fread(in_buffer, sizeof(Record), records_per_chunk, in_file);
  }

  if(read_records > 0)
  {
    free(in_buffer);
    free(out_buffer);
    printf("Total given memory is insufficient\n");
    return (-1);
  }

  return 0;
}

/**
* Compares two records a and b
* with respect to the value of the integer field UID2.
* Returns an integer which indicates relative order:
* positive: record a > record b
* negative: record a < record b
* zero: equal records
*/
int compare (const void *a, const void *b)
{
  int a_uid = ((const Record*)a)->uid2;
  int b_uid = ((const Record*)b)->uid2;
  return (a_uid - b_uid);
}
