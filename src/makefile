db: main.o LoadData.o Index.o ExternalSort.o Query.o
	g++ -g -o db main.o LoadData.o Index.o ExternalSort.o Query.o
LoadData.o: LoadData.cpp LoadData.h
	g++ -c LoadData.cpp
Index.o: Index.cpp Index.h
	g++ -c Index.cpp
ExternalSort.o: ExternalSort.cpp ExternalSort.h
	g++ -c ExternalSort.cpp 
Query.o: Query.cpp Query.h
	g++ -c Query.cpp
main.o: main.cpp LoadData.h Index.h ExternalSort.h Query.h
	g++ -c main.cpp
clean:
	rm db main.o LoadData.o Index.o ExternalSort.o Query.o
