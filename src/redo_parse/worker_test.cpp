#include <iostream>
#include <vector>
#include <list>
#include <memory>
#include <stdio.h>
#include <thread>
#include <signal.h>
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

  void shutdown(int signum) {
    LOG(INFO) << std::endl << "Shutdowning datastream now.. ";
    std::exit(0);
  }

  int main(int ac, char** av) {
    signal(SIGINT, shutdown);
    putenv(const_cast<char*>("NLS_LANG=.AL32UTF8"));
    otl_connect::otl_initialize(1);
    uint32_t startSeq;
    ApplyStats stats;
    try {
      initStream(ac, av);
      stats = ApplierHelper::getApplierHelper().getApplyStats();
      LOG(INFO) << "Last commit Timepoint " << stats.commit_tp_.toString();
      LOG(INFO) << "Restart Timepoint " << stats.restart_tp_.toString();
      startSeq = getLogManager().getSeqFromScn(
          std::to_string(stats.restart_tp_.scn_.toNum()).c_str());
      if (startSeq == 0) {
        LOG(ERROR) << "restart scn is " << stats.restart_tp_.scn_.toStr()
                   << " Can't find out an archived log contains that scn";
        return -10;
      }
      GlobalStream::getGlobalStream().setAppliedSeq(startSeq);
      Transaction::setRestartTimePoint(stats.restart_tp_.scn_,
                                       stats.restart_tp_.epoch_);
      Transaction::setLastCommitTimePoint(stats.commit_tp_.scn_,
                                          stats.commit_tp_.epoch_);
    } catch (otl_exception& p) {
      LOG(ERROR) << p.msg;
      LOG(ERROR) << p.stm_text;
      LOG(ERROR) << p.var_info;
      throw p;
    }
    Monitor m;
    util::guarded_thread t{std::ref(m)};
    startStream(startSeq, stats.restart_tp_);
    return 0;
  }
}

int main(int argc, char** argv) { return databus::main(argc, argv); }
