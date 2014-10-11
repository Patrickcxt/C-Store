#include "Index.h"
#include <stdio.h>
#include <string.h>

Index::Index() {
}

Index::~Index() {
  if (root != NULL) delete root;
}

Index::Index(char* tname) {
  strcpy(tblName, tname);
  init();
}

void Index::buildBtree() {
  int arr[4096];
  int result = 0;
  int pos = 0;
  char p[100];

  cnt = 0;
  create_File_Name(p, indexName, COLFILE);
  pFile = fopen(p, "r+");
  if (pFile == NULL) printf("open pFile error\n");
  create_File_Name(p, indexName, INDEXFILE);
  fd = fopen(p, "wb+");
  if (fd == NULL) printf("open fd error\n");
  root = NULL;

  printf("Begin to create index\n");
  B_Tree_Create();
  
  while ((result = fread(arr, sizeof(int), 4096, pFile)) > 0) {
    for (int i = 0; i < result; i++) {
//      printf("data: %d    pos: %d\n", arr[i], pos);
      B_Tree_Insert(arr[i], pos);
      pos++;
    }
  }

  create_File_Name(p, tblName, INFOFILE);
  FILE* f = fopen(p, "wb+");
  fprintf(f, "%s\n", tblName);
  fprintf(f, "%d\n", colCnt);
  for (int i = 0; i < colCnt; i++)
      fprintf(f,"%s %d %d\n", colName[i], colSize[i], colType[i]);
  fprintf(f, "%d\n", root->page_id);
  printf("root->page_id: %d\n", root->page_id);
  printf("Create Index finish!\n");
  fclose(f);
  fclose(fd);
  fclose(pFile);
}

int Index::Query(int key, int except) {
  int pos = -1;
  if (B_Tree_Search(root, key, pos)) {
    //printf("%-20s: %d\n", colName[0], key);
    for (int i = 0; i < colCnt; i++) {
      if (i == except) continue;
      print_Each_Column(i, pos);
    }
    return true;
  } else {
    printf("KEY %d not Found!\n\n", key);
    return false;
  }
  fclose(fd);
  return pos;
}

bool Index::queryPos(int key, int& pos) {
  return B_Tree_Search(root, key, pos);
}

void Index::init() {
  strcpy(path, "mydata/");
  char p[100];
  create_File_Name(p, tblName, INFOFILE);
  FILE* f = fopen(p, "r");
  char tmp[40];
  fscanf(f, "%s", tmp);
  fscanf(f, "%d", &colCnt);
  for (int i = 0; i < colCnt; i++) {
    int type;
    fscanf(f, "%s %d %d", colName[i], &colSize[i], &type);
    colType[i] = (DataType)type;
  }
  fscanf(f, "%d", &rootid);
  fclose(f);

  strcpy(indexName, colName[0]);
//  printf("root: %d\n", rootid);
}

bool Index::query_Init() {
  char p[100];
  create_File_Name(p, indexName, INDEXFILE);
  fd = fopen(p, "r");
  root = new B_node();
  fseek(fd, sizeof(B_node)*rootid, SEEK_SET);
  fread(root, sizeof(B_node), 1, fd); 
}

void Index::create_File_Name(char* p, char* name, Type type) {
  strcpy(p, path);
  strcat(p, tblName);
  if (type == COLFILE) {
    strcat(p, "_");
    strcat(p, name);
  }
  if (type == INDEXFILE) {
      strcat(p, "_index");
  } 
 // printf("open file: %s\n", p);
}

void Index::print_Each_Column(int i, int pos) {
  char p[100];
  create_File_Name(p, colName[i], COLFILE);
  FILE* f = fopen(p, "r");
  fseek(f, colSize[i]*pos, SEEK_SET);
  if (colType[i] == INT) {
    int value;
    fread(&value, sizeof(int), 1, f);
    printf("%-20s: %d\n", colName[i], value);
  } else if (colType[i] == FLOAT) {
    float value;
    fread(&value, sizeof(float), 1, f);
    printf("%-20s: %.2f\n", colName[i], value);
  } else {
    char str[256];
    char* ptr = NULL;
    fread(str, sizeof(char), colSize[i], f);
    ptr = strchr(str, '*');
    if (ptr == NULL) str[colSize[i]] = '\0';
    else             str[ptr-str] = '\0';
    printf("%-20s: %s\n", colName[i], str);
  }
  fclose(f);
}
//--------------------Btree------------------------------

void Index::Delete(B_node* &ptr) {
  if (ptr != NULL && ptr != root) {
      delete ptr;
      ptr = NULL; 

  }
}

bool Index::B_Tree_Search(B_node* &x, int k, int& position) {
  int i = 0;
  bool ret = false;
  while (i < x->count && k > x->data[i]) {
    i++;
  }
  if (i < x->count && k == x->data[i]) {
    position = x->pos[i];
    ret = true;
  } else if (x->leaf) {
    if (i >= x->count) position = x->pos[i-1] + 1;
    else position = x->pos[i];
    ret = false;
  } else {
    B_node* next = new B_node();
    fseek(fd, sizeof(B_node)*(x->branch[i]), SEEK_SET);
    fread(next, sizeof(B_node), 1, fd);
    ret =  B_Tree_Search(next, k, position);
  }
  Delete(x);
  return ret;
}

/*void Index::B_Tree_Block_Search(B_node* x, int min, int max, int& list[], int& num) {
  int st = 0, ed = 0;
  while (st < x->count && min < x->data[st]) st++; 
  ed = st;
  while (ed < x->count && max >= x->data[ed]) ed++;
  for (int i = st; i <= ed; i++) {
    if (x->branch[i] == -1) continue;
    B_node* next = new B_node();
    fseek(fd, sizeof(B_node)*(x->branch[i]), SEEK_SET);
    fread(next, sizeof(B_node), 1, fd);
    B_Tree_Block_Search(next, min, max, list, num);
    list[num++] = x->data[i];
  } 
  Delete(x); 
}*/

void Index::B_Tree_Create() {
  B_node* x = new B_node();
  x->leaf = true;
  x->page_id = cnt++;
  x->count = 0;
  fwrite(x, sizeof(B_node), 1, fd);
  root = x;
}

bool Index::B_Tree_Split_Child(B_node* &x, int i, B_node* &y) {
  int j;
  B_node* z = new B_node();
  z->leaf = y->leaf;
  z->page_id = cnt++;
  z->count = order - 1;
  // copy the data and position
  for (j = 0; j < order-1; j++) {
    z->data[j] = y->data[j+order];
    z->pos[j] = y->pos[j+order];
  }
  // copy the child of the latter part of y to z
  if (!(y->leaf)) {
    for (j = 1; j < order; j++) {
      z->branch[j] = y->branch[j+order];
    }
  }

  y->count = order - 1;
  for (j = x->count; j >= i+1; j--) {
    x->branch[j+1] = x->branch[j];
  }
  x->branch[i+1] = z->page_id;
  for (j = x->count-1; j >= i; j--) {
    x->data[j+1] = x->data[j];
    x->pos[j+1] = x->pos[j];
  }
  x->data[i] = y->data[order-1];
  x->pos[i] = y->pos[order-1];
  x->count += 1;

  fseek(fd, sizeof(B_node)*(x->page_id), SEEK_SET);
  fwrite(x, sizeof(B_node), 1, fd);

  fseek(fd, sizeof(B_node)*(y->page_id), SEEK_SET);
  fwrite(y, sizeof(B_node), 1, fd);
  Delete(y);

  fseek(fd, sizeof(B_node)*(z->page_id), SEEK_SET);
  fwrite(z, sizeof(B_node), 1, fd);
  Delete(z); 
}



bool Index::B_Tree_Insert_Nonfull(B_node* &x, int k, int position) {
  int i = x->count-1;
  if (x->leaf) {
    while (i >= 0 && k < x->data[i]) {
      x->data[i+1] = x->data[i];
      x->pos[i+1] = x->pos[i];
      i--;
    }
    x->data[i+1] = k;
    x->pos[i+1] = position;
    x->count += 1;
    fseek(fd, sizeof(B_node)*(x->page_id), SEEK_SET);
    fwrite(x, sizeof(B_node), 1, fd);

  } else {
    while (i >= 0 && k < x->data[i])
      i--;
    i++;
    B_node* next = new B_node();
    fseek(fd, sizeof(B_node)*(x->branch[i]), SEEK_SET);        
    fread(next, sizeof(B_node), 1, fd);
    if (next->count == 2*order-1) {
      B_Tree_Split_Child(x, i, next);
      if (k > x->data[i+1]) {
        i++;
      }
    }
    
    if (next == NULL) next = new B_node();
    fseek(fd, sizeof(B_node)*(x->branch[i]), SEEK_SET);
    fread(next, sizeof(B_node), 1, fd);
    B_Tree_Insert_Nonfull(next, k, position);
    Delete(next);
  } 
  return true;
}


bool Index::B_Tree_Insert(int k, int position) { 
  B_node* r = root;        // remember to read from file for r 
  if (r->count == 2*order-1) { 
    B_node* s = new B_node(); 
    root = s;
    s->leaf = false;
    s->page_id = cnt++;
    s->count = 0;
    s->branch[0] = r->page_id;
    B_Tree_Split_Child(s, 0, r);
    B_Tree_Insert_Nonfull(s, k, position);

  } else {
    B_Tree_Insert_Nonfull(r, k, position);
  }
}
