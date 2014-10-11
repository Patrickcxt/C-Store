#ifndef INDEX_H
#define INDEX_H

#include <stdio.h>
#include <stdlib.h>
#include "LoadData.h"
#define order 500

enum Type {
  INFOFILE,  // imformation file
  COLFILE,   // file that stores column data
  INDEXFILE  // indexfile
};

struct B_node {
  bool leaf;
  int count;
  int page_id;
  int data[order*2-1];
  int pos[order*2-1];
  int branch[order*2];
  B_node() {
    leaf = count = 0;
    page_id = -1;
    int i = 0;
    for (i = 0; i < order*2-1; i++)
      data[i] = pos[i] = branch[i] = -1;
    branch[i] = -1;
  }
};

class Index {
  public:
      Index();
      ~Index();
      Index(char* indname);

      //void createMap();
      void buildBtree();
      int Query(int key, int except = -1);
      bool queryPos(int key, int& pos);
      bool query_Init();

  private:
      char tblName[40];     // tablename orders..
      char indexName[40];   // name of index column
      char colName[MaxFile][40]; // name of every column
      char path[100];       // path for mydata
      int colSize[MaxFile];
      DataType colType[MaxFile];
      int colCnt;           // total numbers of columns

      int cnt;
      int rootid;
      B_node* root;
      FILE* fd;            // file of the index
      FILE* pFile;             // file of the index column

      void Delete(B_node* &ptr);
      bool B_Tree_Search(B_node* &x, int k, int& position);
     // void B_Tree_Block_Search(B_node* &x, int min, int max, int& list[], int& num);
      void B_Tree_Create();
      bool B_Tree_Split_Child(B_node* &x, int i, B_node* &y);
      bool B_Tree_Insert_Nonfull(B_node* &x, int k, int position);
      bool B_Tree_Insert(int k, int position);

      void init();
      void create_File_Name(char* p, char* name, Type type);
      void print_Each_Column(int i, int pos);  // not completed
      
};


#endif
