#include <iostream>
#include <vector>
#include <list>
#include <memory>
#include <stdio.h>
#include <thread>
#include "easylogging++.h"
#include "util/container.h"
#include "util/utils.h"
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
#include "monitor.h"

INITIALIZE_EASYLOGGINGPP
namespace databus {
  std::shared_ptr<LogManager> logmanager = NULL;

  std::string getLogfile(uint32_t seq) { return logmanager->getLogfile(seq); }

  uint32_t getOnlineLastBlock(uint32_t seq) {
    return logmanager->getOnlineLastBlock(seq);
  }

  void parseSeq(uint32_t seq, const SCN& restart_scn) {
    RedoFile redofile(seq, getLogfile, getOnlineLastBlock);
    redofile.setStartScn(restart_scn);
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
    auto n = Transaction::removeUncompletedTrans();
    LOG(WARNING) << "removed " << n << " incompleted transaction";

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

    if (!Transaction::start_scn_q_.empty()) {
      Transaction::setRestartScn(*(Transaction::start_scn_q_.begin()));
    }

    LOG(INFO) << "Apply Transaction now " << std::endl;
    auto commit_tran = Transaction::commit_trans_.begin();
    while (commit_tran != Transaction::commit_trans_.end()) {
      SimpleApplier::getApplier(streamconf->getString("tarConn").c_str())
          .apply(commit_tran->second);
      commit_tran = Transaction::commit_trans_.erase(commit_tran);
    }
  }

  int main(int ac, char** av) {
    putenv(const_cast<char*>("NLS_LANG=.AL32UTF8"));
    otl_connect::otl_initialize();
    el::Configurations conf("logging.conf");
    el::Loggers::reconfigureLogger("default", conf);
    util::guarded_thread mt;
    uint32_t startSeq;
    ApplyStats stats;
    try {
      initStream(ac, av);
      stats = ApplierHelper::getApplierHelper().getApplyStats();
      LOG(INFO) << "Got commit scn " << stats.commit_scn_.toStr() << " "
                << stats.commit_scn_.toNum();
      LOG(INFO) << "Got restart scn " << stats.restart_scn_.toStr() << " "
                << stats.restart_scn_.toNum();
      startSeq = logmanager->getSeqFromScn(
          std::to_string(stats.restart_scn_.toNum()).c_str());
      if (startSeq == 0) {
        LOG(ERROR) << "restart scn is " << stats.restart_scn_.toStr()
                   << " Can't find out an archived log contains that scn";
        return -10;
      }
      Transaction::setRestartScn(stats.restart_scn_);
      Transaction::setCommitScn(stats.commit_scn_);
    } catch (otl_exception& p) {
      LOG(ERROR) << p.msg;
      LOG(ERROR) << p.stm_text;
      LOG(ERROR) << p.var_info;
      throw p;
    }
    Monitor m;
    util::guarded_thread t{std::ref(m)};
    try {
      while (true) {
        parseSeq(startSeq, stats.restart_scn_);
        LOG(INFO) << "Transaction appliy completed, "
                  << Transaction::xid_map_.size()
                  << " transactions are pending for appling since they are not "
                     "rollbacked/committed";
        startSeq++;
      }
    } catch (otl_exception& p) {
      LOG(ERROR) << p.msg;
      LOG(ERROR) << p.stm_text;
      LOG(ERROR) << p.var_info;
      throw p;
    }
    return 0;
  }
}

int main(int argc, char** argv) { return databus::main(argc, argv); }
