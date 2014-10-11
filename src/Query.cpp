#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "Query.h"

Query::Query() {

}

Query::~Query() {

}

void Query::indexJoin(const char* firsttbl, const char* secondtbl) {
    openFile(firsttbl, secondtbl, 1);
    printf("indexJoin Begin!\n");
    int i, j, pos1 = 0, pos2 = 0;
    int result = 0;
    int Array[PAGE_SIZE];
    int res[PAGE_SIZE];
    FILE* pFile = fopen("mydata/indexJoin", "w");
    if (pFile == NULL) {
      printf("open pFile error!\n"); return ;
    }
    char samecol[40] = "CUSTKEY";
    char p[100] = "mydata/";
    strcat(p, firtbl);
    strcat(p, "_");
    strcat(p, samecol);
    
    FILE* fq = fopen(p, "r");
    Index Bindex(sectbl);
    Bindex.query_Init();
    int k = 0;
    while ((result = fread(Array, sizeof(int), PAGE_SIZE, fq)) > 0) {
      for (i = 0; i < result; i++) {
        res[k++] = pos1++;
        Bindex.queryPos(Array[i], pos2);
        res[k++] = pos2; 
        if (k >= PAGE_SIZE) {
          fwrite(res, sizeof(int), k, pFile);
          k = 0;
        }
      //  printf("%d:  %d %d %d\n", Array[i], k, pos1-1, pos2);
      } 
    }
    fwrite(res, sizeof(int), k, pFile);
    fclose(fq);
    fclose(pFile);
    printf("indexJoin end!\n");
}

void Query::NJJoin(const char* firsttbl, const char* secondtbl) {
    openFile(firsttbl, secondtbl, 1);
    printf("NJjoin begin!\n");

    int i, j, k, pos1 = 0, pos2 = 0;
    int result = 0;
    int Arr1[PAGE_SIZE], Arr2[PAGE_SIZE], posArr[PAGE_SIZE];
    int res[PAGE_SIZE];
    char samecol[40] = "CUSTKEY";
    char path[100] = "exdata/";
    FILE *f1, *f2, *f3;
    FILE* pFile = fopen("mydata/NJjoin", "w");
    if (pFile == NULL) {
      printf("open pFile error!\n"); return ;
    }

    char p[100];
    strcpy(p, path);
    strcat(p, firtbl);
    strcat(p, "_");
    strcat(p, samecol);
    f1 = fopen(p, "r");
    
    strcpy(p, path);
    strcat(p, firtbl);
    strcat(p, "_");
    strcat(p, samecol);
    strcat(p, "_sortpos");
    f3 = fopen(p, "r");

    strcpy(p, "mydata/");
    strcat(p, sectbl);
    strcat(p, "_");
    strcat(p, samecol);
    f2 = fopen(p, "r");

    int cnt = 0;
    fseek(f1, 0, SEEK_END);
    int lsize = ftell(f1);
    fseek(f1, 0, SEEK_SET);
    fseek(f2, 0, SEEK_SET);
    fseek(f2, 0, SEEK_SET);
    j = PAGE_SIZE; k = 0;
    while ((result = fread(Arr2, sizeof(int), PAGE_SIZE, f2)) > 0) {
      for (i = 0; i < result; i++) {
        while (1) {
          if (j >= PAGE_SIZE) {
            if (cnt*4 >= lsize) break;
            fread(Arr1, sizeof(int), PAGE_SIZE, f1);
            fread(posArr, sizeof(int), PAGE_SIZE, f3);
            j = 0;
          }
          if (Arr2[i] < Arr1[j]) {
            break;
          } else if (Arr2[i] == Arr1[j]) {
            pos1 = posArr[j];
            res[k++] = pos1;
            res[k++] = pos2;
            if (k >= PAGE_SIZE) {
              fwrite(res, sizeof(int), PAGE_SIZE, pFile);
              k = 0;
            }
          }
          j++; 
          cnt++;
        } 
        pos2++;
      }  
      if (cnt*4 >= lsize) break;
    }
    fwrite(res, sizeof(int), k, pFile);

    fclose(f1);
    fclose(f2);
    fclose(f3);
    fclose(pFile);
    printf("NJjoin end!\n");

}

bool Query::openFile(const char* firsttbl,const char* secondtbl, bool jointype) {
    // char* path[100] = "mydata/";
    int i;
    FILE *f1, *f2;
    char path[100] = "mydata/";
    char p[100];
    strcpy(p, path);
    strcpy(firtbl, firsttbl);
    strcat(p, firtbl);
    f1 = fopen(p, "r");
    if (f1 == NULL) printf("open f1 error!\n");
    fscanf(f1, "%s %d", firtbl, &colCnt1);
//    printf("%s %d\n", firtbl, colCnt1);
    int type;
    for (i = 0; i < colCnt1; i++) {
      fscanf(f1, "%s %d %d", colName1[i], &colSize1[i], &type);
      colType1[i] = DataType(type);
    } 
    fscanf(f1, "%d", &root1);
    if (jointype) {
      strcpy(sectbl, secondtbl);
      memset(p, 0, sizeof(p));
      strcpy(p, path);
      strcat(p, sectbl);
      f2 = fopen(p, "r");
      if (f2 == NULL) printf("open f2 error!\n");
      fscanf(f2, "%s %d", sectbl, &colCnt2);
 //     printf("%s %d\n", sectbl, colCnt2);
      for (i = 0; i < colCnt2; i++) {
        fscanf(f2, "%s %d %d", colName2[i], &colSize2[i], &type);
        colType2[i] = DataType(type);
      }
      fscanf(f2, "%d", &root2);
      fclose(f2);
    }
    fclose(f1);
    return true;
}

void Query::queryOneTable(qNode target) {
    openFile(target.tblName, 0);
    char path[100] = "mydata/";
    char p[100];
    strcat(path, firtbl);
    strcat(path, "_");
    FILE* fd[MaxFile];
    FILE* fk;
    char* bufstr[MaxFile];
    int* bufint[MaxFile];
    float* buffloat[MaxFile];
    int bufkey[PAGE_SIZE];  // buffer of the primary key
    int t[MaxFile];
    int i, j, k;
    int a = 0, b = 0, c = 0;
    for (i = 0; i < target.colnum; i++) {
      if (colType1[target.list[i]] == INT) {
        t[i] = a++;
      } else if (colType1[target.list[i]] == FLOAT) {
        t[i] = b++;
      } else {
        t[i] = c++;
      }
      strcpy(p, path);
      strcat(p, colName1[target.list[i]]);
      fd[i] = fopen(p, "r");
      if (fd[i] == NULL) printf("open fd[%d] error!\n", i);
    }
    strcpy(p, path); strcat(p, colName1[0]);
    fk = fopen(p, "r");

    // find the first position
    Index Bindex(target.tblName);
    Bindex.query_Init();
    int inipos = -1;
    bool flag = true;
    Bindex.queryPos(target.min, inipos);
//    printf("inipos : %d\n", inipos);
    for (i = 0; i < target.colnum; i++) {
      fseek(fd[i], colSize1[target.list[i]]*inipos, SEEK_SET);
    }
    fseek(fk, colSize1[0]*inipos, SEEK_SET);
    
    // print the table
    printf("------------------------------------------------------------------------------------------------------------------------------------------\n|");
    for (i = 0; i < target.colnum; i++) {
      printf("%-15s", colName1[target.list[i]]);
      makeExtraSpace(colSize1[target.list[i]]);
    }
    printf("\n");
    while (true) {
      for (k = 0; k < target.colnum; k++) {
        if (colType1[target.list[k]] == INT) {
          
          bufint[t[k]] = (int*)malloc(sizeof(int)*PAGE_SIZE);
          memset(bufint[t[k]], 0, sizeof(bufint[t[k]]));
          fread(bufint[t[k]], sizeof(int), PAGE_SIZE, fd[k]);
        } else if (colType1[target.list[k]] == FLOAT) {
          buffloat[t[k]] = (float*)malloc(sizeof(float)*PAGE_SIZE);
          memset(buffloat[t[k]], 0, sizeof(buffloat[t[k]]));
          fread(buffloat[t[k]], sizeof(float), PAGE_SIZE, fd[k]);
        } else {
          bufstr[t[k]] =(char*)malloc(colSize1[target.list[k]] * PAGE_SIZE);
          memset(bufstr[t[k]], 0, sizeof(bufstr[t[k]]));
          fread(bufstr[t[k]], colSize1[target.list[k]], PAGE_SIZE, fd[k]);
        }
      }
      fread(bufkey, sizeof(int), PAGE_SIZE, fk);

      for (i = 0; i < PAGE_SIZE; i++) {
        printf("------------------------------------------------------------------------------------------------------------------------------------------\n|");
        if (bufkey[i] > target.max) {
          flag = false; break; 
        }
        for (k = 0; k < target.colnum; k++) {
          if (colType1[target.list[k]] == INT) {
            printf("%-15d|", bufint[t[k]][i]); 
          } else if (colType1[target.list[k]] == FLOAT) {
            printf("%-15.2f|", buffloat[t[k]][i]); 
          } else {
            char temp[500];
            strncpy(temp, bufstr[t[k]]+i*colSize1[target.list[k]], colSize1[target.list[k]]); 
            char *ptr = strchr(temp, '*');
            temp[colSize1[target.list[k]]] = '\0';
            while (ptr != NULL && *ptr != '\0') {
              *ptr = ' ';
              ptr++;
            }
            printf("%-15s|", temp);
          }
        }   
        printf("\n");
      }
      if (!flag) break;
      for (i = 0; i < a; i++) free(bufint[i]);
      for (i = 0; i < b; i++) free(buffloat[i]);
      for (i = 0; i < c; i++) free(bufstr[i]);
    } 
    for (i = 0; i < target.colnum; i++) {
      fclose(fd[i]);
    }
}


void Query::queryMultiTable(qNode fir, qNode sec) {
    openFile(fir.tblName, sec.tblName, 1);
    char path1[100] = "mydata/";
    char path2[100] = "mydata/";
    strcat(path1, firtbl);
    strcat(path2, sectbl);
    strcat(path1, "_");
    strcat(path2, "_");
    FILE* fd[MaxFile];
    FILE* fp[MaxFile];
    FILE* pFile = fopen("mydata/indexJoin", "r");
    if (pFile == NULL) printf("open indexjoin error!\n");
    FILE* f1 = fopen("mydata/orders_ORDERKEY", "r");
    if (f1 == NULL) printf("f1\n");
    FILE* f2 = fopen("mydata/customer_CUSTKEY", "r");
    if (f2 == NULL) printf("f2\n");

    char* bufstr[MaxFile];
    int* bufint[MaxFile];
    float* buffloat[MaxFile];
    int posArr[PAGE_SIZE];

    int t[MaxFile];
    int a = 0, b = 0, c = 0;
    int i, k;
    int orderkey, custkey;
    for (i = 0; i < fir.colnum; i++) {
      if (colType1[fir.list[i]] == INT) {
        bufint[a] = NULL;
        t[i] = a++;
      } else if (colType1[fir.list[i]] == FLOAT) {
        buffloat[b] = NULL;
        t[i] = b++;
      } else {
        bufstr[c] = NULL;
        t[i] = c++;
      }
      char p[100];
      strcpy(p, path1);
      strcat(p, colName1[fir.list[i]]);
      fd[i] = fopen(p, "r");
      if (fd[i] == NULL) printf("open fd[%d] error!\n", i);
    }
    for (i = 0; i < sec.colnum; i++) {
      char p[100];
      strcpy(p, path2);
      strcat(p, colName2[sec.list[i]]);
      fp[i] = fopen(p, "r");
      if (fp[i] == NULL) printf("open fp[%d] error!\n", i);
    }

    // print the head of the table
    
    printf("------------------------------------------------------------------------------------------------------------------------------------------\n|");
    for (i = 0; i < fir.colnum; i++) {
      printf("%-15s", colName1[fir.list[i]]);
      makeExtraSpace(colSize1[fir.list[i]]);
    }
    for (i = 0; i < sec.colnum; i++) {
      printf("%-15s", colName2[sec.list[i]]);
      makeExtraSpace(colSize2[sec.list[i]]);
    }
    printf("\n");
    

    int pos = 0, result = 0, j = 0;
    bool flag = true, ini = true;
    while ((result = fread(posArr, sizeof(int), PAGE_SIZE, pFile)) > 0) {
      for (i = 0; i < result; i++) {
        
        j += (posArr[i] - pos);
        pos = posArr[i];
       // read new pages 
        if (j >= PAGE_SIZE || ini) { 
          for (k = 0; k < a; k++) {if (bufint[k] && !ini) {free(bufint[k]); bufint[k] = NULL;}}
          for (k = 0; k < a; k++) {if (buffloat[k] && !ini) {free(buffloat[k]); buffloat[k] = NULL;}}
          for (k = 0; k < a; k++) {if (bufstr[k] && !ini) {free(bufstr[k]); bufstr[k] = NULL;}}
          for (k = 0; k < fir.colnum; k++) {
            fseek(fd[k], colSize1[fir.list[k]]*posArr[i],SEEK_SET);
            if (colType1[fir.list[k]] == INT) {
              bufint[t[k]] = (int*)malloc(sizeof(int)*PAGE_SIZE);
              memset(bufint[t[k]], 0, sizeof(bufint[t[k]]));
              int r = fread(bufint[t[k]], sizeof(int), PAGE_SIZE, fd[k]);
            } else if (colType1[fir.list[k]] == FLOAT) {
              buffloat[t[k]] = (float*)malloc(sizeof(float)*PAGE_SIZE);
              memset(buffloat[t[k]], 0, sizeof(buffloat[t[k]]));
              int r = fread(buffloat[t[k]], sizeof(float), PAGE_SIZE, fd[k]);
            } else {
              bufstr[t[k]] =(char*)malloc(colSize1[fir.list[k]] * PAGE_SIZE);
              memset(bufstr[t[k]], 0, sizeof(bufstr[t[k]]));
              int r = fread(bufstr[t[k]], colSize1[fir.list[k]], PAGE_SIZE, fd[k]);
            }
          }
          j = 0; ini = false;
        }
        
     //   printf("posArr[i]:%d  posArr[i+1]:%d\n", posArr[i], posArr[i+1]);
        fseek(f1, sizeof(int)*posArr[i], SEEK_SET);
        fread(&orderkey, sizeof(int), 1, f1);
        fseek(f2, sizeof(int)*posArr[++i], SEEK_SET);
        fread(&custkey, sizeof(int), 1, f2);
      //  printf("%d %d\n", orderkey, custkey); 
        if (orderkey >= fir.max) { flag = false; break; }
        if (orderkey <= fir.min || custkey <= sec.min || custkey >= sec.max) continue;

        printf("------------------------------------------------------------------------------------------------------------------------------------------\n|");
        
        for (k = 0; k < fir.colnum; k++) {
          if (colType1[fir.list[k]] == INT) {
            printf("%-15d|", bufint[t[k]][j]); 
          } else if (colType1[fir.list[k]] == FLOAT) {
            printf("%-15.2f|", buffloat[t[k]][j]); 
          } else {
            char temp[500];
            strncpy(temp, bufstr[t[k]]+i*colSize1[fir.list[k]], colSize1[fir.list[k]]); 
            char *ptr = strchr(temp, '*');
            temp[colSize1[fir.list[k]]] = '\0';
            while (ptr != NULL && *ptr != '\0') {
              *ptr = ' ';
              ptr++;
            }
            printf("%-15s|", temp);
          }
        }   
        print_One_Tuple(fp, sec, posArr[i], colSize2, colType2);
      }
      if (!flag) break;
    } 
    printf("------------------------------------------------------------------------------------------------------------------------------------------\n|");
    
    for (i = 0; i < fir.colnum; i++) {
      fclose(fd[i]);
    }
    for (i = 0; i < sec.colnum; i++) {
      fclose(fp[i]);
    }
    fclose(pFile);
    fclose(f1);
    fclose(f2);

}

bool Query::queryEqualTuple(qNode Q) {
    openFile(Q.tblName);
    FILE* fp[MaxFile];
    char path[100] = "mydata/";
    strcat(path, firtbl);
    strcat(path, "_");
    for (int i = 0; i < Q.colnum; i++) {
      char p[100];
      strcpy(p, path);
      strcat(p, colName1[Q.list[i]]);
      fp[i] = fopen(p, "r");
    }
    Index Bindex(firtbl);
    Bindex.query_Init();
    int pos = -1;
    Bindex.queryPos(Q.min, pos);
    if (pos == -1) {
        printf("Not Found!\n");
        return false;
    }
    printf("------------------------------------------------------------------------------------------------------------------------------------------\n|");
    for (int i = 0; i < Q.colnum; i++) {
      printf("%-15s", colName1[Q.list[i]]);
      makeExtraSpace(colSize1[Q.list[i]]);
    }
    printf("\n");
    printf("------------------------------------------------------------------------------------------------------------------------------------------\n|");
    print_One_Tuple(fp, Q, pos, colSize1, colType1);
    printf("------------------------------------------------------------------------------------------------------------------------------------------\n|");
    for (int i = 0; i < Q.colnum; i++)
        fclose(fp[i]);
    return true;
}

void Query::print_One_Tuple(FILE** fp, qNode Q, int pos, int* colSize, DataType* colType) {
    int i;
    for (i =0; i < Q.colnum; i++) {
      if (fp[i] == NULL) printf("fp[%d] error\n", i);
      fseek(fp[i], pos*colSize[Q.list[i]], SEEK_SET);
      if (colType[Q.list[i]] == INT) {
        int value;
        fread(&value, sizeof(int), 1, fp[i]) ;
        printf("%-15d|", value);
      } else if (colType[Q.list[i]] == FLOAT) {
        float value;
        fread(&value, sizeof(float), 1, fp[i]);
        printf("%-15.2f|", value);
      } else {
        char str[256];
        char* ptr = NULL;
        fread(str, sizeof(char), colSize[Q.list[i]], fp[i]);
        ptr = strchr(str, '*');
        str[colSize[Q.list[i]]] = '\0';
        while (ptr != NULL && *ptr != '\0') {
          *ptr = ' ';
          ptr++;
        }
        printf("%-15s|", str);
      }
    }
    printf("\n");
}

void Query::makeExtraSpace(int size) {
  if (size > 15) {
    for (int i = 15; i < size; i++)
        putchar(' ');
  }
  printf("|");
}
