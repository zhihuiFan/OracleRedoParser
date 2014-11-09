/*
 * =====================================================================================
 *
 *       Filename:  dump_op.cpp
 *
 *    Description:  Dump Op of a given logfile
 *
 *        Version:  1.0
 *        Created:  10/14/2014 16:30:27
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zhifan (Zhihui Fan), zhihuifan@163.com
 *   Organization:
 *
 * =====================================================================================
 */
#include <stdlib.h>
#include <map>
#include <iostream>
#include "workers.h"
#include "redofile.h"
#include "logical_elems.h"
#include "physical_elems.h"

namespace databus {
  SList record_pointers;
  RecordList record_bufs;

  int main(int argc, char** argv) {
    Reader reader(&record_pointers);
    reader.feed(argv[1]);
    reader.srun();
    std::cout << "total record " << record_pointers.size() << std::endl;
    std::map<Ushort, unsigned long> total_count;
    RecordBuilder builder(&reader, &record_bufs);
    builder.srun();
    std::cout << "build record " << record_bufs.size() << std::endl;
    while (!record_bufs.empty()) {
      RecordBuf* record_buf = record_bufs.pop_front();
      if (record_buf != NULL) {
        for (auto change : record_buf->change_vectors) {
          if (change != NULL) {
            Ushort opcode = change->opCode();
            total_count[opcode]++;
          }
        }
      }
    }
    for (auto i : total_count) {
      std::cout << std::hex << i.first << "\t" << std::dec << i.second << "\n";
    }
    return 0;
  }
}
int main(int argc, char** argv) { return databus::main(argc, argv); }
