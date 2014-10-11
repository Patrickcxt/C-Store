#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#define buffer_size 4096

int main()
{
  int i;
  int arr[1024];
  char s[30];
  scanf("%s", s);
  FILE* fd = fopen(s, "r");
  FILE* f = fopen("output", "w");
  int result = 0;
  while ((result = fread(arr, sizeof(int), 1024, fd)) > 0) {
    for (i = 0; i < result; i++)
        fprintf(f, "%d\n", arr[i]);
  }
  fclose(fd);
  fclose(f);
  return 0;
}
