#ifndef QUERY_H
#define QUERY_H
#include "Index.h"
#include "LoadData.h"

#define PAGE_SIZE 4096

struct qNode {
  char tblName[40];
  int list[MaxFile];
  int colnum;
  int min, max;
};

class Query {
public:
    Query();
    ~Query();

    void indexJoin(const char* firsrttbl, const char* secondtbl);   
    void NJJoin(const char* firsttbl, const char* secondtbl);
    void queryOneTable(qNode target);
    void queryMultiTable(qNode first, qNode second);
    bool queryEqualTuple(qNode Q);
private:

    char firtbl[40], sectbl[40];
    char colName1[MaxFile][40], colName2[MaxFile][40];
    int colCnt1, colCnt2;
    int colSize1[MaxFile], colSize2[MaxFile];
    DataType colType1[MaxFile], colType2[MaxFile];
    int root1, root2;
    FILE* fc[MaxFile];

    bool openFile(const char* firsttbl, const char* sectbl = "", bool jointype = 0);
    void print_One_Tuple(FILE** fp, qNode Q, int pos, int* colSize, DataType* colType);
    void makeExtraSpace(int size);
};

#endif
