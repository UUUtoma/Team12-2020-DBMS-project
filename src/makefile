# 编译加了libpmem库的源文件时加上-l pmem参数进行编译
# 在这个makefile中添加ycsb测试的相关编译命令，运行ycsb

# Where to find user code.
USER_HEADER_DIR = ../include

# Flags passed to the C++ compiler.
CXXFLAGS += -g -pthread -std=c++11

BIN = bin

DATA = ../data

YCSB = ./bin/ycsb

$(YCSB): ycsb.o pm_ehash.o data_page.o | $(BIN)
	${CXX} ${CXXFLAGS} $^ -lpmem -o $@

%.o: %.cpp
	${CXX} ${CXXFLAGS} -I ${USER_HEADER_DIR} -o $@ -c $^


$(BIN):
	mkdir -p bin

$(DATA):
	mkdir -p ../data

clean :
	rm -f $(YCSB) *.o ../data/*

cleano :
	rm -f *.o

cleand : 
	rm -f ../data/*
