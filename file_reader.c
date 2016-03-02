#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "Merge.h"

int main(int argc, char *argv[])
{
  if(argc != 2)
  {
    printf("Usage: file_reader <input .dat file>\n");
    return (-1);
  }

  FILE *in_file;
  if(!(in_file = fopen(argv[1], "rb")))
  {
    printf("Unable to open file \"%s\"\n", argv[1]);
    return (-1);
  }

  Record record;
  while(fread(&record, sizeof(Record), 1, in_file))
  {
    printf("%d,%d\n", record.uid1, record.uid2);
  }

  fclose(in_file);
  return 0;
}
