#include <iostream>
#include <vector>
#include <list>
#include <memory>
#include "util/container.h"
#include "stream.h"
#include "metadata.h"
#include "tconvert.h"
#include "redofile.h"
#include "opcode.h"
#include "util/logger.h"
#include "opcode_ops.h"
#include "trans.h"
#include "otlv4.h"

namespace databus {

  std::shared_ptr<LogManager> logmanager = NULL;

  std::string getLogfile(uint32_t seq) { return logmanager->getLogfile(seq); }

  uint32_t getOnlineLastBlock(uint32_t seq) {
    return logmanager->getOnlineLastBlock(seq);
  }
  int main(int ac, char** av) {
    otl_connect::otl_initialize();
    try {
      initStream(ac, av);
    } catch (otl_exception& p) {
      std::cerr << p.msg << std::endl;  // print out error message
      std::cerr << p.stm_text
                << std::endl;  // print out SQL that caused the error
      std::cerr << p.var_info
                << std::endl;  // print out the variable that caused the error
      throw p;
    }
    RedoFile redofile(getStreamConf().getUint32("startSeq"), getLogfile,
                      getOnlineLastBlock);
    RecordBufPtr buf;
    unsigned long c = 0;
    while ((buf = redofile.nextRecordBuf()).get()) {
      addToTransaction(buf);
      ++c;
    }
    debug() << "total record found  = " << c << std::endl;

    info() << "Build Transaction now" << std::endl;
    for (auto tran : Transaction::xid_map_) {
      int r = tran.second->buildTransaction();
      switch (r) {
        case 0:
          info() << "Transaction " << tran.second->xid_
                 << " doesn't end, check it later" << std::endl;
          break;
        case 1:
          info() << "Transaction " << tran.second->xid_ << " rollbacked "
                 << std::endl;
          break;

        case 2:
          info() << "Transaction " << tran.second->xid_
                 << " Committed, will apply it soon" << std::endl;
          break;
      }
    }

    info() << "Apply Transaction now " << std::endl;
    for (auto tran : Transaction::commit_trans_) {
      std::cout << tran.second->toString() << std::endl;
    }

    //    MetadataManager::destoy();
    return 0;
  }
}

int main(int argc, char** argv) { return databus::main(argc, argv); }
