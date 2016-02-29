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

  /*****************************************************************************
  ***   PHASE 1 (2.2)
  *****************************************************************************/
  Record *in_buffer = (Record *)(calloc(records_per_chunk, sizeof(Record)));

  char i_string[10];
  int read_records = fread(in_buffer, sizeof(Record), records_per_chunk, in_file);
  for(i = 0; i < num_runs; i++)
  {
    qsort(in_buffer, records_per_chunk, sizeof(Record), compare);

    out_file = fopen(strcat(i_string, OUTPUT_FILE_SUFFIX), "wb");
    fwrite(in_buffer, sizeof(Record), read_records, out_file);
    fclose(out_file);

    read_records = fread(in_buffer, sizeof(Record), records_per_chunk, in_file);
  }

  fclose(in_file);
  free(in_buffer);

  if(read_records > 0)
  {
    printf("Total given memory is insufficient\n");
    return (-1);
  }

  /*****************************************************************************
  ***     PHASE 2 (2.3)
  *****************************************************************************/
  /* Arrays to hold buffers, file pointers, and indexes in array */
  FILE *run_files[num_runs];
  int cur_i[num_runs];
  Record *runs[num_runs];
  Record *out_buffer;

  /* Compute new chunk size with accounting for output buffer */
  chunk_size = total_mem / (num_runs + 1);
  if(chunk_size % block_size > 0)
  {
    chunk_size -= chunk_size % block_size;
  }
  /* Calculate records in each chunk (used for buffer and runs) */
  records_per_chunk = chunk_size / sizeof(Record);
  if(records_per_chunk == 0)
  {
    printf("Too many runs or block size too large\n");
    return (-1);
  }

  /* Allocate all buffers */
  for(i = 0; i < num_runs; i++)
  {
    runs[i] = (Record *)(calloc(records_per_chunk, sizeof(Record)));
    cur_i[i] = 0;

    sprintf(i_string, "%d", i);
    run_files[i] = fopen(strcat(i_string, OUTPUT_FILE_SUFFIX), "rb");
  }
  out_file = fopen(OUTPUT_FILE_NAME, "wb");
  out_buffer = (Record *)(calloc(records_per_chunk, sizeof(Record)));

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
