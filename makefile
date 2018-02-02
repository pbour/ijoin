OS := $(shell uname)
ifeq ($(OS),Darwin)
        CC      = /usr/local/opt/llvm/bin/clang++
        CFLAGS  = -O3 -mavx -std=c++14 -w -march=native -I/usr/local/opt/llvm/include -fopenmp
        LDFLAGS = -L/usr/local/opt/llvm/lib
else
        CC      = g++
        CFLAGS  = -O3 -mavx -std=c++14 -w -march=native -fopenmp
        LDFLAGS =
endif

SOURCES = containers/relation.cpp containers/bucket_index.cpp algorithms/nls.cpp algorithms/fs.cpp algorithms/gfs.cpp algorithms/bgfs.cpp partitioning/hash_based.cpp partitioning/domain_based.cpp scheduling/greedy.cpp
OBJECTS = $(SOURCES:.cpp=.o)

all: main

main: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) main.cpp -o ij $(LDADD)


.cpp.o:
	$(CC) $(CFLAGS) -c $< -o $@

.cc.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf containers/*.o
	rm -rf algorithms/*.o
	rm -rf partitioning/*.o
	rm -rf scheduling/*.o
	rm -rf ij

