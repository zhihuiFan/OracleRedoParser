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
#include "redofile.h"
#include "logical_elems.h"

int main(int argc, char** argv) {
  std::map<short, uint32_t> opmap;
  databus::RedoFile redofile(argv[1]);
  databus::RecordBuf* buf = NULL;
  while ((buf = redofile.nextRecordBuf()) != NULL) {
    for (auto change : buf->change_vectors) {
      opmap[change->opCode()] += 1;
    }
  }

  for (auto i : opmap) {
    std::cout << std::hex << "0x" << i.first << "->" << std::dec << i.second
              << std::endl;
  }
}
