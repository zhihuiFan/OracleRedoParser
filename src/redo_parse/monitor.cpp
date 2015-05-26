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
      ah.saveApplyProgress(Transaction::getLastCommitScn(),
                           Transaction::getRestartScn());
      sleep(3);
    }
  }
}
