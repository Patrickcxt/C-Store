#include "LoadData.h"
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <stdio.h>
#include <fstream>
#define buffer_size 4096

//char buffer[128][buffer_size]

LoadData::LoadData() {

}
LoadData::~LoadData() {

}

LoadData::LoadData(char* s) {
  strcpy(tblname, s);
  strcpy(path, "data/");
  strcat(path, s);
  strcat(path, ".tbl");
  getCatalog();
}

bool LoadData::load_Catalog_to_File() {
  char p[100] = "mydata/";
  strcat(p, tblname);
  FILE* pFile = fopen(p, "w");
  if (pFile == NULL) {
    printf("open table %s error!\n", tblname);
    return false;
  }
  fprintf(pFile, "%s\n", tblname);
  fprintf(pFile, "%d\n", tot_col);
  for (int i = 0; i < tot_col; i++) {
    fprintf(pFile, "%s %d %d\n", col[i], col_size[i], colType[i]);
    tuple_size += col_size[i];
  }
  int rootid = -1;
  fprintf(pFile, "%d\n", rootid);
  fclose(pFile);
  return true;
}

bool LoadData::load_Columns_to_File() {
  int i;
  
  // create buffer
  char buf[4*buffer_size];
 // char *tmpbuf[20];
  char* bufStr[MaxFile] = {0};
  int*  bufArr[MaxFile] = {0};
  float* bufFloat[MaxFile] = {0};
  
  int tmpsize[20];
  int cur[20] = {0};
  FILE *fd[20];	
  printf("ready to load columns to file...\n");
	
  FILE* f = fopen(path, "r");
  if (f == NULL) {
	printf("open %s error\n", path);
	return 0;
  }
  // create files 
  for (i = 0; i < tot_col; i++) { 
    char p[100] = "mydata/";
    strcat(p, colname[i]);
	fd[i] = fopen(p, "w"); 
	if (fd[i] == NULL) {
		printf("open %s error\n", colname[i]);
		return 0;
	}
  }
		// loading...
  printf("-------------\n");
  int offset = 0, result = 0;
  const char c = '|';
  char* ptr = NULL;
  
  int k = 0;
  while ((result = fread(buf, 1, 4*buffer_size, f)) > 0) { 
	offset = 0;
    for (i = 0; i < typeCnt[0]; i++) {
      bufArr[i] = NULL;
      bufArr[i] = (int*)malloc(4*buffer_size);
      memset(bufArr[i], 0, sizeof(bufArr[i]));
      if (bufArr[i] == NULL) printf("bufArr[%d] error!\n", i);
    }
    for (i = 0; i < typeCnt[1]; i++) {
      bufFloat[i] = NULL;
      bufFloat[i] = (float*)malloc(4*buffer_size);
      memset(bufFloat[i], 0, sizeof(bufFloat[i]));
      if (bufFloat[i] == NULL) printf("bufFloat[%d] error!\n", i);
    }
    for (i = 0; i < typeCnt[2]; i++) {
      bufStr[i] = NULL;
      bufStr[i] = (char*)malloc(4*buffer_size);
      memset(bufStr[i], 0, sizeof(bufStr[i]));
      if (bufStr[i] == NULL) printf("bufStr[%d] error!\n", i);
    }
    memset(cur, 0, sizeof(cur));
	while (result) {
		if (buf[offset] == '\n') { offset++;} 
		ptr = strchr(buf+offset, c);
		if (ptr == NULL || offset >= result) {
		    break;
		}
		char temp[256];
		memset(temp, 0, sizeof(temp));
        strncpy(temp, buf+offset, ptr-buf-offset+1);
	    //printf("%s\n", temp);
        int length = ptr-buf-offset;
		temp[length] = '\0';
        //printf("%s\n", temp);
        if (colType[k] == INT) {
          int value = atoi(temp);
          bufArr[colOrder[k]][cur[k]++] = value;         
       //   printf("%s: %d\n", col[k], bufArr[colOrder[k]][cur[k]-1]);
       //   getchar();
        } else if (colType[k] == FLOAT) {
          float value = atof(temp);
          bufFloat[colOrder[k]][cur[k]++] = value;
       //   printf("%s: %.2f\n", col[k], value);
        } else {
          for (i = length; i < col_size[k]; i++)
             temp[i] = '*';
          temp[i] = '\0';
          strcat(bufStr[colOrder[k]], temp); 
        //  printf("%s: %s\n", col[k], temp);
          cur[k]++;
        }
		offset = ptr-buf+1;
		k = (k+1)%tot_col; 
    }
			 
	for (i = 0; i < tot_col; i++) {
        //printf("%d\n", tmpsize[i]);
		//fwrite(tmpbuf[i], 1, tmpsize[i], fd[i]);
        printf("cur: %d\n", cur[i]);
        if (colType[i] == INT) {
          fwrite(bufArr[colOrder[i]], sizeof(int), cur[i], fd[i]);
          if (bufArr[colOrder[i]] != NULL) free(bufArr[colOrder[i]]);
        }

        if (colType[i] == FLOAT) {
          fwrite(bufFloat[colOrder[i]], sizeof(float), cur[i], fd[i]);
          if (bufFloat[colOrder[i]] != NULL) free(bufFloat[colOrder[i]]);
        }
        if (colType[i] == STRING) {
          fwrite(bufStr[colOrder[i]], sizeof(char), cur[i]*col_size[i], fd[i]);
          if (bufStr[colOrder[i]] != NULL) free(bufStr[colOrder[i]]);
        }
	}
	int dif = offset-result;

    printf("offset **************************************%d\n", offset);
	fseek(f, dif, SEEK_CUR);

  }
  fclose(f);
  for (i = 0; i < tot_col; i++)
    fclose(fd[i]);
    
  printf("Load finish!\n");
  return true;
}

void LoadData::getCatalog() {
  char type[10];
  int a = 0, b = 0, c = 0;
  printf("input attribute:\n");
  scanf("%d", &tot_col);
  for (int i = 0; i < tot_col; i++) {
    scanf("%s %d %s", col[i], &col_size[i], type);
  //  printf("%s\n", col[i]);
  //  printf("%s %d %s\n", col[i], col_size[i], type);
	strcpy(colname[i], tblname);
	strcat(colname[i], "_");
	strcat(colname[i], col[i]);
    
    if (!strcmp(type, "INT")) {
        colType[i] = INT;
        colOrder[i] = a++;
        typeCnt[0] = a;
    } else if (!strcmp(type, "FLOAT")) {
        colType[i] = FLOAT;
        colOrder[i] = b++;
        typeCnt[1] = b;
    } else {
        colType[i] = STRING;
        colOrder[i] = c++;
        typeCnt[2] = c;
    }
	//	printf("%s\n", colname[i]);
  }
}

