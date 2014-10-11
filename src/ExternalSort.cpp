#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include "ExternalSort.h"
#include <queue>
using namespace std;

struct node {
  int val, pos;
  node(int _val, int _pos) : val(_val), pos(_pos) {}
};

struct cmp {
  bool operator ()(pair<int, node>& a, pair<int, node>& b){
      return a.second.val > b.second.val;
  }
};

ExternalSort::ExternalSort() {

}

ExternalSort::~ExternalSort() {

}

ExternalSort::ExternalSort(char* tName, char* cName) {
    strcpy(path, "exdata/");
    strcat(path, tName);
    strcat(path, "_");
    strcat(path, cName);
    strcpy(tblName, tName);
    strcpy(colName, cName);
    page_cnt = 0;
}

void ExternalSort::Sort() {
    //system("rm ~/cstore/exdata/orders_CUSTKEY");
    system("cp -f ./mydata/orders_CUSTKEY ./exdata/");
    fd = fopen(path, "r+");
    if (fd == NULL) printf("open fd error!\n");
    Page_Sort();
    Merge();
    fclose(fd);
}

void ExternalSort::Page_Sort() {
    int result = 0;
    int Arr[PAGE_SIZE];
    int posArr[PAGE_SIZE];
    int offset = 0;
    int pos = 0;
    int sum1 = 0, sum2 = 0;
    bool flag;
    printf("Page Sort Begin!\n");
   // printf("fd: %ld  fq: %ld", &fd, &fq);
    fq = fopen("exdata/orders_CUSTKEY_sortpos", "w+");
    if (fq == NULL) printf("open fq error!\n");
    fseek(fd, 0, SEEK_SET);
    fseek(fq, 0, SEEK_SET);
    while ((result = fread(Arr, sizeof(int), PAGE_SIZE, fd)) > 0) {
      for (int i =0; i < result; i++) posArr[i] = pos++;
      quick_Sort(Arr, 0, result-1, posArr);
    
     /* for (i = 0; i < result; i++) {
        
      }*/      
      fseek(fd, offset, SEEK_SET);
      fseek(fq, offset, SEEK_SET);
      sum1 += fwrite(Arr, sizeof(int), result, fd);
      sum2 += fwrite(posArr, sizeof(int), result, fq);
      offset += result*4;
      fseek(fd, offset, SEEK_SET);
      fseek(fq, offset, SEEK_SET);
      page_cnt++;
    }
    fseek(fd, 0, SEEK_SET);
    fseek(fq, 0, SEEK_SET);
    printf("sum1: %d sum2: %d\n", sum1, sum2);
    printf("Page Sort Done!\n");
    fclose(fq);
    return ;
}

void ExternalSort::Merge() {
    int i = 0, k = 1, j = 0;
    int pass = 1;
    printf("Merge Begin!\n");
    fq = fopen("exdata/orders_CUSTKEY_sortpos", "r+");
    if (fq == NULL) printf("open fq error!\n");
   // page_cnt = 367;
    while (1) {
      // get the number of pages to be merge in this round basee on current k;
      int tcnt = page_cnt/k;
      if (page_cnt % k != 0) tcnt++;
      if (tcnt <= 1) break;  //only one page to be merged ,done!
      i = 0; 
      printf("---------------------PASS %d-------------------------------------\n", pass++);
      printf("page_cnt: %d tcnt: %d\n", page_cnt, tcnt);
      while (1) {
        // for each round, get 16(curcnt) pages to merge
        int curcnt = 0;
        if (tcnt <= 0) break;
        if (tcnt >= Ways) {
          curcnt = Ways;
          tcnt -= Ways;
        } else {
          curcnt = tcnt;
          tcnt -= curcnt;
        }
       
        // get the start position and numbers of unit page for each pages 
        for (j = 0; j < curcnt; j++) {
           posAtFile[j] = i + j * k;
           if (page_cnt-posAtFile[j] < k)
               blockCnt[j] = page_cnt - posAtFile[j];
           else blockCnt[j] = k;
           printf("page:%d posAtFile: %d blockCnt: %d \n", j, posAtFile[j], blockCnt[j]);
        }
        printf("%d pages to be merged!\n", curcnt);
        merge_sort(i, curcnt, k);
        printf("---------------------------------------------------------------\n\n");
        i = i + curcnt*k;
      }
      k *= Ways;  
    } 
    printf("Merge Finish!\n\n\n");
    fclose(fq);
}

void ExternalSort::merge_sort(int st, int cnt, int pages) {
    
    int i, j;
    int curpos[Ways];  
    int curcnt[Ways];  // now have read curcnt numbers
    int rcnt[Ways];    // read rcnt numbers from this pages

    int Arr[Ways][PAGE_SIZE];  // buffer of key
    int posArr[Ways][PAGE_SIZE]; // buffer of postion
    int result[PAGE_SIZE];     // store the result after sorting
    int presult[PAGE_SIZE];    // store the result of positon after sorting
    bool finish[Ways];         // flag whether numbers of the page have all been read 
    FILE* pFile = fopen("exdata/tempdata.dat", "w+"); // temp file to store the result
    FILE* ff = fopen("exdata/temppos.dat", "w+");
    if (pFile == NULL) printf("open pFile error!\n");
    if (ff == NULL) printf("open ff error!\n");
    priority_queue<pair<int, node>,vector<pair<int, node> >, cmp >que;
    
    // in the beginning, read a buffer of data for each 'page'
    for (i = 0; i < cnt; i++) {
      fseek(fd, PAGE_SIZE*4*posAtFile[i], SEEK_SET);
      fseek(fq, PAGE_SIZE*4*posAtFile[i], SEEK_SET);
      rcnt[i] = fread(Arr[i], sizeof(int), PAGE_SIZE, fd);
      fread(posArr[i], sizeof(int), PAGE_SIZE, fq);
      curpos[i] = posAtFile[i];
//      printf("Arr: %d posArr: %d\n", Arr[i][0], posArr[i][0]);
      que.push(pair<int, node>(i, node(Arr[i][0], posArr[i][0])));
      finish[i] = false;
    }      
    
    int t = 0;
    int sum = 0;
    memset(curcnt, 0, sizeof(curcnt));
    while (true) {
      for (i = 0; i < cnt; i++)
          if (!finish[i]) break;
      if (i == cnt) break;  // all pages have all been read
      pair<int, node>P = que.top();
      que.pop();
      node nd = P.second;
      result[t++] = nd.val;
      presult[t-1] = nd.pos;
     // printf("%d %d\n", nd.val, nd.pos);
     // getchar();

      if (t == PAGE_SIZE) {
        sum += fwrite(result, sizeof(int), PAGE_SIZE, pFile);
        int num = fwrite(presult, sizeof(int), PAGE_SIZE, ff);
        t = 0;
      }
      i = P.first;
      if (++curcnt[i] >= rcnt[i] && !finish[i]) {
        curpos[i]++;
        if (curpos[i]-posAtFile[i] >= blockCnt[i]) {
          finish[i] = true;          
        } else {
          fseek(fd, PAGE_SIZE*4*curpos[i], SEEK_SET);
          fseek(fq, PAGE_SIZE*4*curpos[i], SEEK_SET);
          rcnt[i] = fread(Arr[i], sizeof(int), PAGE_SIZE, fd);
          fread(posArr[i], sizeof(int), PAGE_SIZE, fq);
          que.push(pair<int, node>(i, node(Arr[i][0], posArr[i][0])));
          curcnt[i] = 0;
        }
            
      } else {
        que.push(pair<int, node>(i, node(Arr[i][curcnt[i]], posArr[i][curcnt[i]]))); // !!!
      }
      
    }
    if (t) {
      sum += fwrite(result, sizeof(int), t, pFile);
      fwrite(presult, sizeof(int), t, ff);
    }

    printf("total numbers have been merged: %d\n", sum);
    // Write Back
    int num = 0;
    fseek(pFile, 0, SEEK_SET);
    fseek(ff, 0, SEEK_SET);
    fseek(fd, PAGE_SIZE*4*st, SEEK_SET);
    fseek(fq, PAGE_SIZE*4*st, SEEK_SET);
    while ((num = fread(result, sizeof(int), PAGE_SIZE, pFile)) > 0) {
      fwrite(result, sizeof(int), num, fd);
      num = fread(presult, sizeof(int), PAGE_SIZE, ff);
      fwrite(presult, sizeof(int), num, fq);
    }
    fclose(pFile);
    fclose(ff);

}

void ExternalSort::RLECompress() {
    int Arr[PAGE_SIZE];
    int comArr[PAGE_SIZE];
    int result;
    int num = -1;
    int cnt = 0;
    char p[100];
    strcpy(p, path);
    strcat(p, "_COM");
    fd = fopen(path, "r");
    fp = fopen(p, "w");
    int i, j, k = 0;
    int sum = 0;
    printf("Compress Begin!\n");
    while ((result = fread(Arr, sizeof(int), PAGE_SIZE, fd)) > 0) {
      i = 0;
      while (i < result) {
        if (Arr[i] == num) {
          cnt++;
        } else {
          if (num != -1) {
            comArr[k++] = num;
            comArr[k++] = cnt;
            sum += cnt;
            if (k == PAGE_SIZE) {
              fwrite(comArr, sizeof(int), k, fp);
              k = 0;
            }
          }
          num = Arr[i]; cnt = 1;
        }
        i++;
      } 
    }
    comArr[k++] = num; comArr[k++] = cnt; sum += cnt;
    fwrite(comArr, sizeof(int), k, fp);
    fclose(fp);
    fclose(fd);
    printf("%d numbers has been compressed!\n", sum);
    printf("Compress Finish!\n");
}

void ExternalSort::quick_Sort(int st[], int p, int r, int pst[]) {
    
    if (p >= r) {
       return;
    }

    int j = p-1;
    int i = p; 
    int key = st[r];
    for (; i < r; i++) {
      if (st[i] <= key) {
        int temp = st[++j]; st[j] = st[i]; st[i] = temp; 
        temp = pst[j]; pst[j] = pst[i]; pst[i] = temp; 
      }
    }
    int q = j+1;
    int temp = st[r]; st[r] = st[q]; st[q] = temp;
    temp = pst[r]; pst[r] = pst[q]; pst[q] = temp;
    quick_Sort(st, p, q-1, pst);
    quick_Sort(st, q+1, r, pst);
}
