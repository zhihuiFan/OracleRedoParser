CXXFLAGS_BASE=-I. -I.. -std=c++11 -g -O0  -pthread
CC=g++
DEPS=metadata.h \
	 logical_elems.h \
	 opcode.h \
	 opcode_ops.h \
	 physical_elems.h \
	 redofile.h \
     workers.h \
	 stream.h \
	 stream_error.h \
	 trans.h

OCI_FLAGS=

LDFLAGS_BASE=-lpthread
OCI_FLAGS=-locci -lclntsh

PLATFORM=$(shell uname)
ifeq ($(PLATFORM),SunOS)
	LDFLAGS=-L${OCCI_HOME} -static-libstdc++ -static-libgcc ${LDFLAGS_BASE} -pthread /usr/local/lib/libboost_log.a /usr/local/lib/libboost_log_setup.a  /usr/local/lib/libboost_thread.a /usr/local/lib/libboost_system.a -lrt
	CXXFLAGS = ${CXXFLAGS_BASE} -static-libstdc++ -static-libgcc
endif

ifeq ($(PLATFORM), Darwin)
	OCI_FALGS=-DMAC_OSX -D_GNU_SOURCE -D_REENTRANT -mmacosx-version-min=10.10
	CXXFLAGS = ${CXXFLAGS_BASE} -I/Users/zhifan/Downloads/instantclient_11_2/sdk/include/
	LDFLAGS= ${LDFLAGS_BASE} -L/Users/zhifan/Downloads/instantclient_11_2 -lnnz11 -lboost_log-mt
endif

ifeq ($(PLATFORM), Linux)
	CXXFLAGS = ${CXXFLAGS_BASE} -I/home/oracle/instantclient_11_2/sdk/include/ -DBOOST_LOG_DYN_LINK
	LDFLAGS=${LDFLAGS_BASE} -L/home/oracle/instantclient_11_2 -L/home/oracle/app/oracle/product/11.2.0/dbhome_1 -lnnz11 -lboost_log -lboost_program_options
endif

%.o: %.cpp $(DEPS)
	$(CC) -c -o $@ $< $(CXXFLAGS)

datastream: stream.o logical_elems.o physical_elems.o redofile.o worker_test.o opcode_ops.o stream.o metadata.o tconvert.o trans.o
	${CC} ${CXXFLAGS} -o $@ $^ ${LDFLAGS} $(OCI_FLAGS)

dumpop: logical_elems.o physical_elems.o redofile.o opcode_ops.o dump_op.o
	${CC} ${CXXFLAGS} -o $@ $^ ${LDFLAGS}

metadata.o: metadata.cpp $(DEPS)
	$(CC) -c -o $@ $< $(CXXFLAGS) $(OCI_FALGS)

testmeta: metadata.o testmeta.o
	${CC} ${CXXFLAGS} -o $@ $^ ${LDFLAGS}

myocci: myocci.o
	${CC} ${CXXFLAGS} -o $@ $^ ${LDFLAGS}

.PHONY: clean

clean:
	@rm -f *.o datastream dumpop
