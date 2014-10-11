#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define buffer_size 4096

int main()
{
  int i;
  int length;
  char arr[4096];
  char s[30];
  char com[500];
  scanf("%s %d", s, &length);
  FILE* fd = fopen(s, "r");
  FILE* fp = fopen("output", "w");
  int result = 0;
  int pos = 0;
  while ((result = fread(arr, sizeof(char), 4096, fd)) > 0) {
    int offset = 0;
    while (result) {
      if (offset >= result) break;
      memset(com, 0, sizeof(com));
//      printf("%s\n", com);
      strncpy(com, arr+offset, length);
      offset += length; 
      pos++;
     // if (pos % 100000 == 0)
      fprintf(fp, "pos: %d -- %s\n", pos, com);
    //  getchar();
    }
    int dif = offset - result;
    fseek(fd, dif, SEEK_CUR);
  }
  fclose(fd);
  return 0;
}
