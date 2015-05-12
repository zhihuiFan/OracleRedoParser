#include <iostream>
#include <vector>
#include <list>
#include <memory>
#include <stdio.h>
#include "easylogging++.h"
#include "util/container.h"
#include "stream.h"
#include "metadata.h"
#include "tconvert.h"
#include "redofile.h"
#include "opcode.h"
#include "util/logger.h"
#include "opcode_ops.h"
#include "trans.h"
#include "applier.h"
#include "otlv4.h"

INITIALIZE_EASYLOGGINGPP
namespace databus {
  std::shared_ptr<LogManager> logmanager = NULL;

  std::string getLogfile(uint32_t seq) { return logmanager->getLogfile(seq); }

  uint32_t getOnlineLastBlock(uint32_t seq) {
    return logmanager->getOnlineLastBlock(seq);
  }
  int main(int ac, char** av) {
    putenv(const_cast<char*>("NLS_LANG=.AL32UTF8"));
    otl_connect::otl_initialize();
    el::Configurations conf("logging.conf");
    el::Loggers::reconfigureLogger("default", conf);
    // el::Loggers::reconfigureAllLoggers(conf);
    //  try {
    initStream(ac, av);
    RedoFile redofile(getStreamConf().getUint32("startSeq"), getLogfile,
                      getOnlineLastBlock);
    RecordBufPtr buf;
    unsigned long c = 0;
    while ((buf = redofile.nextRecordBuf()).get()) {
      if (buf->change_vectors.empty()) continue;
      addToTransaction(buf);
      ++c;
      if (c % 1000 == 0) {
        LOG(DEBUG) << "Parsed " << c << " Records ";
      }
    }
    LOG(INFO) << "total record found  = " << c << std::endl;

    LOG(INFO) << "Build Transaction now" << std::endl;
    auto tran = Transaction::xid_map_.begin();
    while (tran != Transaction::xid_map_.end()) {
      auto it = buildTransaction(tran);
      if (it != Transaction::xid_map_.end()) {
        tran = it;
      } else {
        tran++;
      }
    }

    LOG(INFO) << "Apply Transaction now " << std::endl;
    // for (auto tran : Transaction::commit_trans_) {
    auto commit_tran = Transaction::commit_trans_.begin();
    while (commit_tran != Transaction::commit_trans_.end()) {
      if (!commit_tran->second->empty()) {
        LOG(INFO) << commit_tran->second->toString();
        SimpleApplier::getApplier(streamconf->getString("tarConn").c_str())
            .apply(commit_tran->second);
      }
      commit_tran = Transaction::commit_trans_.erase(commit_tran);
    }
    //    MetadataManager::destoy();
    /*
  } catch (otl_exception& p) {
    std::cerr << p.msg << std::endl;  // print out error message
    std::cerr << p.stm_text
              << std::endl;  // print out SQL that caused the error
    std::cerr << p.var_info
              << std::endl;  // print out the variable that caused the error
    throw p;
  } */
    return 0;
  }
}

int main(int argc, char** argv) { return databus::main(argc, argv); }
