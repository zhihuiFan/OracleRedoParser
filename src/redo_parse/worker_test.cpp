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

namespace databus {

  std::shared_ptr<LogManager> logmanager = NULL;

  std::string getLogfile(uint32_t seq) { return logmanager->getLogfile(seq); }

  uint32_t getOnlineLastBlock(uint32_t seq) {
    return logmanager->getOnlineLastBlock(seq);
  }
  int main(int ac, char** av) {
    initStream(ac, av);
    RedoFile redofile(getStreamConf().getUint32("startSeq"), getLogfile,
                      getOnlineLastBlock);
    RecordBufPtr buf;
    unsigned long c = 0;
    while ((buf = redofile.nextRecordBuf()).get()) {
      addToTransaction(buf);
      ++c;
    }
    debug() << " total record found  = " << c << std::endl;
    debug() << " build sql record " << std::endl;

    info() << "Dump Transaction now" << std::endl;
    for (auto tran : Transaction::xid_map_) {
      verifyTrans(tran.second);
      if (!tran.second->changes_.empty()) {
        info() << tran.second->toString() << std::endl;
      }
    }
    //    MetadataManager::destoy();
    return 0;
  }
}

int main(int argc, char** argv) { return databus::main(argc, argv); }
