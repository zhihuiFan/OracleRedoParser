#include <unistd.h>

#include "monitor.h"
#include "applier.h"
#include "trans.h"
#include "stream.h"
#include "easylogging++.h"

namespace databus {
  void Monitor::operator()() {
    while (true) {
      ApplierHelper& ah = ApplierHelper::getApplierHelper();
      try {
        ah.saveApplyProgress(Transaction::getLastCommitTimePoint(),
                             Transaction::getRestartTimePoint());
      } catch (otl_exception& p) {
        LOG(ERROR) << p.msg;
        LOG(ERROR) << p.stm_text;
        LOG(ERROR) << p.var_info;
      }
      sleep(3);
    }
  }
}
