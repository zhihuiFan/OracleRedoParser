CXXFLAGS=-I. -I.. -std=c++11 -g -O0 -I${OCCI_HOME}/sdk/include
CC=g++
DEPS=metadata.h \
	 logical_elems.h \
	 opcode.h \
	 opcode_ops.h \
	 physical_elems.h \
	 redofile.h \
     workers.h \
	 stream.h \

OCI_FLAGS=

LDFLAGS=-lpthread -lboost_program_options

PLATFORM=$(shell uname)
ifeq ($(PLATFORM),SunOS)
    CXXFLAGS=-I. -I.. -g -O0 -std=c++11 
	LD_SIMPLE=-static-libstdc++ -static-libgcc
	LDFLAGS=-L${OCCI_HOME} -lpthread ${LD_SIMPLE}
endif

ifeq ($(PLATFORM), Darwin)
	OCI_FALGS=-DMAC_OSX -D_GNU_SOURCE -D_REENTRANT -mmacosx-version-min=10.10
	LDFLAGS=-lpthread -lboost_program_options -L/Users/zhifan/Downloads/instantclient_11_2 -locci -lclntsh -lpthread
endif

ifeq ($(PLATFORM), Linux)
	OCI_FALGS=-L/home/oracle/instantclient_11_2 -L/home/oracle/app/oracle/product/11.2.0/dbhome_1 -locci -lclntsh -lpthread -lnnz11
	LDFLAGS=-lpthread -lboost_program_options -L/Users/zhifan/Downloads/instantclient_11_2 -locci -lclntsh -lpthread
endif

%.o: %.cpp $(DEPS)
	$(CC) -c -o $@ $< $(CXXFLAGS)

datastream: stream.o logical_elems.o physical_elems.o redofile.o workers.o worker_test.o opcode_ops.o stream.o metadata.o tconvert.o
	${CC} ${CXXFLAGS} $(OCI_FALGS) -o $@ $^ ${LDFLAGS} 

dumpop: logical_elems.o physical_elems.o redofile.o workers.o opcode_ops.o dump_op.o
	${CC} ${CXXFLAGS} -o $@ $^ ${LD_SIMPLE}

metadata.o: metadata.cpp $(DEPS)
	$(CC) -c -o $@ $< $(CXXFLAGS) $(OCI_FALGS)

testmeta: metadata.o testmeta.o
	${CC} ${CXXFLAGS} -o $@ $^ ${LDFLAGS}

myocci: myocci.o
	${CC} ${CXXFLAGS} -o $@ $^ ${LDFLAGS}

.PHONY: clean

clean:
	@rm -f *.o datastream dumpop
