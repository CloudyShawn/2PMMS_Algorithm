#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "Merge.h"

int main(int argc, char *argv[])
{
  /* Check inputs */
  if(argc != 5)
  {
    printf("Usage: disk_sort <name of the input file> <total mem in bytes> <block size> <number of runs>\n");
    return (-1);
  }

  /*****************************************************************************
  ***     PHASE 1 (2.2) Splitting original file
  *****************************************************************************/
  /* set up sorting manager */
  SortingManager *sorter = (SortingManager *)(calloc(1, sizeof(SortingManager)));
  sorter->totalPartitions = atoi(argv[4]);
  sorter->totalRecords = 0;
  if(!(sorter->inputFile = fopen(argv[1], "rb")))
  {
    printf("Could not open file %s for reading\n", argv[1]);
    return -1;
  }

  /* Calculate if runs are too big for given memory */
  fseek(sorter->inputFile, 0, SEEK_END);
  long file_size = ftell(sorter->inputFile);
  fseek(sorter->inputFile, 0, SEEK_SET);

  long run_size = file_size / sorter->totalPartitions;
  run_size -= run_size % sizeof(Record);
  if(run_size > atoi(argv[2]))
  {
    printf("Run sizes are too large to fit into given memory space\n");
    return (-1);
  }

  /* split file into seperate run files */
  int i;
  char filename[10];
  long records_per_run = run_size / sizeof(Record);
  sorter->partitionBuffer = (Record *)(calloc(records_per_run, sizeof(Record)));
  sorter->totalRecords = fread(sorter->partitionBuffer, sizeof(Record), records_per_run, sorter->inputFile);
  for(i = 0; i < sorter->totalPartitions; i++)
  {
    qsort(sorter->partitionBuffer, sorter->totalRecords, sizeof(Record), compare);

    sprintf(filename, "%s%d", OUTPUT_FILE_PREFIX, i);
    FILE *out_file = fopen(filename, "wb");
    fwrite(sorter->partitionBuffer, sizeof(Record), sorter->totalRecords, out_file);
    fclose(out_file);

    sorter->totalRecords = fread(sorter->partitionBuffer, sizeof(Record), records_per_run, sorter->inputFile);
  }

  /* free all allocated, unneeded variables and close file pointers */
  free(sorter->partitionBuffer);
  fclose(sorter->inputFile);

  /*****************************************************************************
  ***     PHASE 2 (2.3) Merging files
  *****************************************************************************/


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

/* merges all runs into a single sorted list */
int mergeRuns (MergeManager *merger)
{

}
/* initial fill of input buffers with elements of each run */
int initInputBuffers(MergeManager *merger)
{

}

/* inserts into heap one element from each buffer - to keep the smallest on top */
int initHeap(MergeManager *merger)
{

}

/* reads the next element from an input buffer */
int getNextRecord (MergeManager *merger, int run_id, Record *result)
{

}

/* uploads next part of a run from disk if necessary by calling refillBuffer */
int refillBuffer(MergeManager *merger, int run_id)
{

}

/* inserts next element from run run_id into heap */
int insertIntoHeap (MergeManager *merger, int run_id, Record *newRecord)
{

}

/* removes smallest element from the heap, and restores heap order */
int getTopHeapElement (MergeManager *merger, HeapRecord *result)
{

}

/* adds next smallest element to the output buffer, flushes buffer if full by calling flushOutputBuffer */
int addToOutputBuffer(MergeManager *merger, Record * newRecord)
{

}

int flushOutputBuffer(MergeManager *merger)
{

}
