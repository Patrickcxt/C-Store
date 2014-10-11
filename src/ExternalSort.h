#ifndef EXTERNAL_SORT_H
#define EXTERNAL_SORT_H

#define Ways 16
#define PAGE_SIZE 4096

class ExternalSort {
public:
    ExternalSort();
    ExternalSort(char* tName, char* cName);
    ~ExternalSort();    

    void Sort();
    void RLECompress();

private:
    char path[100];
    char tblName[40];
    char colName[40];
    
    int page_cnt;
    int posAtFile[Ways];
    int blockCnt[Ways];
    FILE* fd;
    FILE* fp;
    FILE* fq;

    void Page_Sort();
    void quick_Sort(int st[], int p, int r, int* pst);
    void Merge();
    void merge_sort(int st, int cnt, int pages);
    //void qSort();
   
};

#endif
