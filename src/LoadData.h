#ifndef LOADDATA_H
#define LOADDATA_H
#define MaxFile 40
enum DataType {
  INT,
  FLOAT,
  STRING
};

class LoadData {
    public:
    LoadData();
    ~LoadData();
    LoadData(char* s);
     
    bool load_Columns_to_File();
    bool load_Catalog_to_File();

 private:
    
    char tblname[40];     // name of this table
	char colname[MaxFile][40]; // file name of columns
    char col[MaxFile][40];     // name of columns
    int col_size[MaxFile];     // size of columns
    DataType colType[MaxFile]; // Data type
    int colOrder[MaxFile];     //
    int typeCnt[3];       // numbers for each data type;

	char path[100];       // path for the data
    int tot_col;    
    int tot_record;
    int tuple_size;

  	void getCatalog();
//    bool load_Columns_byIndex(int x);
};

#endif
