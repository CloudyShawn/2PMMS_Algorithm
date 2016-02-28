/**
	* Arguments:
	* 1 - an array to sort
	* 2 - size of an array
	* 3 - size of each array element
	* 4 - function to compare two elements of the array

	qsort (buffer, total_records, sizeof(Record), compare);
*******************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include "Merge.h"

int main(int argc, char *argv[])
{
  if(argc != 5)
  {
    printf("Usage: <name of the input file> <total mem in bytes> <block size> <number of runs>\n");
    return 0;
  }

  char *input_filename = argv[1];
  unsigned int total_mem = atoi(argv[2]);
  int block_size = atoi(argv[3]);
  int num_runs = atoi(argv[4]);

  FILE *in_file;
  if(!(in_file = fopen(input_filename, "rb")))
  {
    printf("Could not open file %s for reading\n", input_filename);
    return -1;
  }

  Record *buffer = (Record *)(calloc(100, sizeof(Record)));
  fread(buffer, sizeof(Record), 100, in_file);

  qsort(buffer, 100, sizeof(Record), compare);

  int i;
  for(i = 0; i < 100; i++)
  {
    printf("%d,%d\n", buffer[i].uid2, buffer[i].uid1);
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
