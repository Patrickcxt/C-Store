#include <cstdio>
#include <iostream>
#include <cstring>
#include <map>
#include <cstdlib>
#include <string>
#include <unistd.h>
#include "LoadData.h"
#include "Index.h"
#include "ExternalSort.h"
#include "Query.h"

using namespace std;

void breakIntoTokens(char* command, char token[][40], int& cnt);
void queryDeal(char token[][40], int cnt, int& type, qNode& Q1, qNode& Q2);
void init();

map<string, int> Map;
int main(int argc, char** argv)
{
    char command[200];
    char token[20][40];
    int cnt = 0;

    printf("Please input a command!\n");
    fgets(command, 200, stdin);

    breakIntoTokens(command, token, cnt);

    if (strcmp(token[0], "LOAD") == 0) { // load tables
      printf("token: %s\n", token[1]);
      LoadData data(token[1]);
      data.load_Catalog_to_File();
      data.load_Columns_to_File();
    } else if (strcmp(token[0], "INDEX") == 0) { // creat the Btree Index
      Index Bindex(token[1]);
      Bindex.buildBtree();
      
    } else if (strcmp(token[0], "COMPRESS") == 0) { //ExternalSort and compress the data
      ExternalSort EX(token[1], token[2]);
      EX.Sort();
      EX.RLECompress();

    } else if (strcmp(token[0], "NJJOIN") == 0) {
      Query* q = new Query();
      q->NJJoin(token[1], token[2]);
    } else { // SELECT QUERY
      int type;
      init();
      qNode Q1, Q2;
      Q1.min = 0; Q1.max = 0x7fffffff;
      Q2.min = 0; Q2.max = 0x7fffffff;
      queryDeal(token, cnt, type, Q1, Q2);
  /*    printf("tblName: %s %s\n", Q1.tblName, Q2.tblName);
      printf("colnum: %d %d\n", Q1.colnum, Q2.colnum);
      for (int i = 0; i < Q1.colnum; i++)
          printf("%d ", Q1.list[i]);
      printf("\n");
      for (int i = 0; i < Q2.colnum; i++)
          printf("%d ", Q2.list[i]);
      printf("\n");
      printf("min max: %d %d %d %d\n", Q1.min, Q1.max, Q2.min, Q2.max);
   */
      Query* q = new Query();
      if (type == 0) {
        q->queryEqualTuple(Q1);
      } else if (type == 1) {
        q->queryOneTable(Q1);
      } else  {
        if (access("./mydata/indexJoin", 0) == -1) 
          q->indexJoin(Q1.tblName, Q2.tblName);
        q->queryMultiTable(Q1, Q2);
      }
      delete q; 
      
    }
    return 0;
}

void init() {
  Map["orders.ORDERKEY"] = 0;
  Map["orders.CUSTKEY"] = 1;
  Map["orders.ORDERSTATUS"] = 2;
  Map["orders.TOTALPRICE"] = 3;
  Map["orders.ORDERDATE"] = 4;
  Map["orders.ORDERPRIORITY"] = 5;
  Map["orders.CLERK"] = 6;
  Map["orders.SHIPPRIORITY"] = 7;
  Map["orders.COMMENT"] = 8;

  Map["customer.CUSTKEY"] = 9;
  Map["customer.NAME"] = 10;
  Map["customer.ADDRESS"] = 11;
  Map["customer.NATIONKEY"] = 12;
  Map["customer.PHONE"] = 13;
  Map["customer.ACCTBAL"] = 14;
  Map["customer.MKTSEGMENT"] = 15;
  Map["customer.COMMENT"] = 16;
}

void breakIntoTokens(char* command, char token[][40], int& cnt) {
  int len = strlen(command);
  int j = 0;
  for (int i = 0; i <= len; i++) {
    if (command[i] == ' ' || command[i] == '\0' || command[i] == '\n') {
      token[cnt++][j++] = '\0';
      
      j = 0;
    } else {
      token[cnt][j++] = command[i];
    }
  } 
}


void queryDeal(char token[][40], int cnt, int& type, qNode& Q1, qNode& Q2) {
  int i, j, a = 1000, b = 1000, c = 0;
  string temp;
  for (i = 0; i < cnt; i++) {
    if (strcmp(token[i], "FROM") == 0) a = i;
    if (strcmp(token[i], "WHERE") == 0) b = i; 
  }
  if (b - a == 2) {
    type = 1;
    strcpy(Q1.tblName, token[a+1]);
    int num = 0;
    for (i = 1; i < a; i++){
      temp = token[a+1];
      temp += ".";
      temp += token[i];
      Q1.list[num++] = Map[temp] % 9;
      
    }
    Q1.colnum = num;
    for (i = b+1; i < cnt; i++) {
      if (strcmp(token[i], "=") == 0) {
          Q1.min = atoi(token[++i]);
          type = 0; return; 
      } else if (strcmp(token[i], ">") == 0) {
          Q1.min =atoi(token[++i]) + 1;
      } else if (strcmp(token[i], "<") == 0){
          Q1.max = atoi(token[++i]) -1;
      }
    }
  } else {
    type = 2;
    strcpy(Q1.tblName, token[a+1]);
    strcpy(Q2.tblName, token[a+2]);
    int num1 = 0, num2 = 0;
    for (i = 1; i < a; i++) {
      temp = token[i];
      if (Map[temp] > 8) {
        Q2.list[num2++] = (Map[temp]) % 9;
      } else {
        Q1.list[num1++] = Map[temp];
      }
    }
    Q1.colnum = num1; Q2.colnum = num2;
    for (i = b+1; i < cnt; i++) {
      if (strcmp(token[i],"ORDERKEY") == 0) {
         if (strcmp(token[++i], ">") == 0) 
          Q1.min = atoi(token[++i])+1;
         else if (strcmp(token[i], "<") == 0)
          Q1.max = atoi(token[++i])-1;
       } else if (strcmp(token[i], "CUSTKEY") == 0) {
         if (strcmp(token[++i], ">") == 0)
          Q2.min = atoi(token[++i])+1;
         else if (strcmp(token[i], "<") == 0)
          Q2.max = atoi(token[++i])-1;
       }
    }
  } 
}
