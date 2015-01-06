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

  int main(int ac, char** av) {
    initStream(ac, av);
    RedoFile redofile(
        getStreamConf().getUint32("startSeq"),
        [](uint32_t seq) -> std::string { return logmanager->getLogfile(seq); },
        [](uint32_t seq)
            -> uint32_t { return logmanager->getOnlineLastBlock(seq); });
    RecordBufPtr buf;
    unsigned long c = 0;
    while ((buf = redofile.nextRecordBuf()) != NULL) {
      buildTransaction(buf);
      ++c;
    }
    BOOST_LOG_TRIVIAL(debug) << " total record found  = " << c;
    BOOST_LOG_TRIVIAL(info) << "Dump Transaction now";
    for (auto tran : Transaction::getXIDMap()) {
      verifyTrans(tran.second);
      BOOST_LOG_TRIVIAL(info) << tran.second->toString();
    }
    //    MetadataManager::destory();
    return 0;
  }
}

int main(int argc, char** argv) { return databus::main(argc, argv); }
