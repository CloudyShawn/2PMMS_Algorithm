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
  InputBuffer *inputBuffers = (InputBuffer *)(calloc(sorter->totalPartitions, sizeof(InputBuffer)));
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
    printf("%s written to disk\n", filename);

    inputBuffers[i].filename = calloc(strlen(filename) + 1, sizeof(char));
    strcpy(inputBuffers[i].filename, filename);
    inputBuffers[i].runLength = sorter->totalRecords * sizeof(Record);

    sorter->totalRecords = fread(sorter->partitionBuffer, sizeof(Record), records_per_run, sorter->inputFile);
  }

  /* free all allocated, unneeded variables and close file pointers */
  free(sorter->partitionBuffer);
  fclose(sorter->inputFile);

  /*****************************************************************************
  ***     PHASE 2 (2.3) Merging files
  *****************************************************************************/
  /* Figure out new buffer capacities */
  long buffer_size = atoi(argv[2]) / (sorter->totalPartitions + 1);
  buffer_size -= buffer_size % atoi(argv[3]);
  if(buffer_size == 0)
  {
    printf("Too many runs for the amount of memory given\n");
    return (-1);
  }

  MergeManager *merger = (MergeManager *)(calloc(1, sizeof(MergeManager)));
  merger->heapCapacity = sorter->totalPartitions;
  merger->heap = (HeapRecord *)(calloc(merger->heapCapacity, sizeof(HeapRecord)));
  merger->heapSize = 0;
  merger->inputFP = NULL;
  merger->outputFP = fopen(OUTPUT_FILE_NAME, "wb");
  merger->outputBuffer = (Record *)(calloc(buffer_size / sizeof(Record), sizeof(Record)));
  merger->currentPositionInOutputBuffer = 0;
  merger->outputBufferCapacity = buffer_size / sizeof(Record);
  merger->inputBuffers = inputBuffers;

  for(i = 0; i < merger->heapCapacity; i++)
  {
    merger->inputBuffers[i].capacity = merger->outputBufferCapacity;
    merger->inputBuffers[i].currentPositionInFile = 0;
    merger->inputBuffers[i].currentBufferPosition = 0;
    merger->inputBuffers[i].done = 0;
    merger->inputBuffers[i].buffer = (Record *)(calloc(merger->inputBuffers[i].capacity, sizeof(Record)));
  }

  mergeRuns(merger);


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
	/* Fill initial buffers and fill in heap from each buffer */
	initInputBuffers(merger);
	initHeap(merger);

  /* Keep going until heap is empty */
	while(merger->heapSize > 0)
  {
    /* save head of heap and append to output buffer */
		HeapRecord head;
    getTopHeapElement(merger, &head);
    addToOutputBuffer(merger, &head);

    /* Check if run from removed record on heap has any more records left */
    if(merger->inputBuffers[head.run_id].currentBufferPosition < merger->inputBuffers[head.run_id].totalElements)
    {
      /* Get next record from run and insert it into heap */
      Record new_record;
      getNextRecord(merger, head.run_id, &new_record);
      insertIntoHeap(merger, head.run_id, &new_record);
    }
    else
    {
      /* Remove run from heap, causing heapSize to drop */
      removeRun(merger);
    }
	}

	/* flush what remains in output buffer */
	if(merger->currentPositionInOutputBuffer > 0)
  {
		flushOutputBuffer(merger);
	}

	return 0;
}

/* initial fill of input buffers with elements of each run */
int initInputBuffers(MergeManager *merger)
{
  /* Loop through all runs and fill in buffers */
  int i;
  for(i = 0; i < merger->heapCapacity; i++)
  {
    /* easier name to use for current buffer instead of merger->inputBuffers[i] */
    InputBuffer *cur_buffer = &(merger->inputBuffers[i]);

    /* open text file for reading */
    if(!(merger->inputFP = fopen(cur_buffer->filename, "rb")))
    {
      printf ("Could not open file \"%s\" for reading \n", cur_buffer->filename);
      return (-1);
    }
    /* Read into buffer and save total read records */
    cur_buffer->totalElements = fread(cur_buffer->buffer, sizeof(Record), cur_buffer->capacity, merger->inputFP);
    /* set new position to read in file */
    cur_buffer->currentPositionInFile = ftell(merger->inputFP);

    fclose (merger->inputFP);
  }

  return 0;
}

/* inserts into heap one element from each buffer - to keep the smallest on top */
int initHeap(MergeManager *merger)
{
  int i;
  for(i=0; i < merger->heapCapacity; i++)
  {
    merger->heap[i].uid1 = merger->inputBuffers[i].buffer[merger->inputBuffers[i].currentBufferPosition].uid1;
    merger->heap[i].uid2 = merger->inputBuffers[i].buffer[merger->inputBuffers[i].currentBufferPosition].uid2;
    merger->heap[i].run_id = i;
    merger->inputBuffers[i].currentBufferPosition++;
    merger->heapSize++;
  }
  qsort(merger->heap, merger->heapSize, sizeof(HeapRecord), compare);
  return 0;
}

/* reads the next element from an input buffer */
int getNextRecord (MergeManager *merger, int run_id, Record *result)
{
  result->uid1 = merger->inputBuffers[run_id].buffer[merger->inputBuffers[run_id].currentBufferPosition].uid1;
  result->uid2 = merger->inputBuffers[run_id].buffer[merger->inputBuffers[run_id].currentBufferPosition].uid2;

  merger->inputBuffers[run_id].currentBufferPosition++;

  if(merger->inputBuffers[run_id].currentBufferPosition == merger->inputBuffers[run_id].totalElements)
  {
    refillBuffer(merger, run_id);
  }

  return 0;
}

/* uploads next part of a run from disk if necessary by calling refillBuffer */
int refillBuffer(MergeManager *merger, int run_id)
{
  if(merger->inputBuffers[run_id].done == 0)
  {
      merger->inputFP = fopen(merger->inputBuffers[run_id].filename, "rb");
      fseek(merger->inputFP, merger->inputBuffers[run_id].currentPositionInFile, SEEK_SET);
      merger->inputBuffers[run_id].totalElements = fread(merger->inputBuffers[run_id].buffer, sizeof(Record), merger->inputBuffers[run_id].capacity, merger->inputFP);
      merger->inputBuffers[run_id].currentPositionInFile = ftell(merger->inputFP);
      if(merger->inputBuffers[run_id].currentPositionInFile == merger->inputBuffers[run_id].runLength)
      {
        merger->inputBuffers[run_id].done = 1;
      }
      merger->inputBuffers[run_id].currentBufferPosition = 0;
  }
  return 0;
}

/* inserts next element from run run_id into heap */
int insertIntoHeap (MergeManager *merger, int run_id, Record *newRecord)
{
  /* Record all new record data into first slot of heap (recently removed slot) */
  merger->heap[0].uid1 = newRecord->uid1;
  merger->heap[0].uid2 = newRecord->uid2;
  merger->heap[0].run_id = run_id;

  /* resort heap */
  qsort(merger->heap, merger->heapSize, sizeof(HeapRecord), compare);

  return 0;
}

/* removes smallest element from the heap, and restores heap order */
int getTopHeapElement (MergeManager *merger, HeapRecord *result)
{
  /* record all values of first item in heap into output result variable */
  result->uid1 = merger->heap[0].uid1;
  result->uid2 = merger->heap[0].uid2;
  result->run_id = merger->heap[0].run_id;

  return 0;
}

/* adds next smallest element to the output buffer, flushes buffer if full by calling flushOutputBuffer */
int addToOutputBuffer(MergeManager *merger, HeapRecord *newRecord)
{
  /* Copy new record to output buffer */
  merger->outputBuffer[merger->currentPositionInOutputBuffer].uid1 = newRecord->uid1;
  merger->outputBuffer[merger->currentPositionInOutputBuffer].uid2 = newRecord->uid2;

  /* increment output buffer index */
  merger->currentPositionInOutputBuffer++;

  /* Flush output buffer if buffer is full */
  if (merger->currentPositionInOutputBuffer == merger->outputBufferCapacity)
  {
    flushOutputBuffer(merger);
  }

  return 0;
}

int flushOutputBuffer(MergeManager *merger)
{
  /* Write all contents of output buffer out and reset index */
  fwrite(merger->outputBuffer, sizeof(Record), merger->currentPositionInOutputBuffer, merger->outputFP);
  merger->currentPositionInOutputBuffer = 0;

  return 0;
}

/* drops capacity of heap and removes top element */
int removeRun(MergeManager *merger)
{
  /* Remove temporary run files */
  remove(merger->inputBuffers[merger->heap[0].run_id].filename);
  printf("Removed %s%d\n", OUTPUT_FILE_PREFIX, merger->heap[0].run_id);

  /* subtract from heapsize to remove a run */
  merger->heapSize--;

  /* copy info from final heap item to first removed heap item */
  merger->heap[0].uid1 = merger->heap[merger->heapSize].uid1;
  merger->heap[0].uid2 = merger->heap[merger->heapSize].uid2;
  merger->heap[0].run_id = merger->heap[merger->heapSize].run_id;

  return 0;
}
