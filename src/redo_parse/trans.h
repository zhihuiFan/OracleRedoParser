#ifndef TRANS_INC
#define TRANS_INC
#include <map>
#include <set>
#include <vector>
#include <memory>
#include <string>
#include <sstream>
#include <mutex>
#include <atomic>
#include <algorithm>
#include "easylogging++.h"
#include "logical_elems.h"
#include "opcode.h"
#include "util/dtypes.h"
#include "logical_elems.h"

namespace databus {

  struct ColumnLess {
    bool operator()(const std::shared_ptr<ColumnChange>& l,
                    const std::shared_ptr<ColumnChange>& r) {
      return l->col_id_ < r->col_id_;
    }
  };

  const std::set<char> cflag{0x00, 0x10, 0x02, 0x12, 0x04, 0x14};

  struct RowChange;
  class TabDef;
  typedef std::shared_ptr<RowChange> RowChangePtr;
  typedef std::set<ColumnChangePtr, ColumnLess> OrderedPK;

  struct RowChange {
    RowChange();
    RowChange(SCN& scn, uint32_t obj_id, Ushort op, Ushort uflag, Ushort iflag,
              Row& undo, Row& redo);
    bool operator<(const RowChange& other) const { return scn_ < other.scn_; }
    std::string toString() const;
    std::string pkToString() const;
    std::vector<std::string> getPk();
    bool completed() const;

    SCN scn_;
    uint32_t object_id_;
    Ushort op_;
    Ushort start_col_;
    // we only care about cc for 11.6
    // see https://jirap.corp.ebay.com/browse/DBISTREA-37
    Ushort cc_;
    Ushort uflag_;
    Uchar iflag_;
    OrderedPK old_pk_;
    OrderedPK new_pk_;
  };

  struct Less {
    bool operator()(const std::shared_ptr<RowChange>& l,
                    const std::shared_ptr<RowChange>& r) {
      return l->scn_ < r->scn_;
    }
  };

  struct Transaction;
  typedef std::map<XID, std::shared_ptr<Transaction>> XIDMap;
  typedef std::map<DBA, USN> DBAMap;

  struct Transaction {
   public:
    Transaction() : cflag_(-1), xid_(0), start_scn_(), commit_scn_() {}
    XID xid_;
    SCN start_scn_;
    SCN commit_scn_;
    char cflag_;

    // orginize changes, for row-chains, row migration
    std::set<RowChangePtr, Less> changes_;
    std::string toString() const;

    bool operator<(const Transaction& other) const;
    bool has_rollback() const { return cflag_ & 4; }
    bool has_commited() const { return !(still_pending() || has_rollback()); }
    bool still_pending() const { return cflag_ == -1; }
    bool empty() const { return changes_.empty(); }
    void tidyChanges();

    static XIDMap xid_map_;
    static std::map<SCN, std::shared_ptr<Transaction>> commit_trans_;

   private:
    void merge(RowChangePtr r);
    bool lastCompleted() const;

   public:
    static DBAMap dba_map_;
    static std::set<SCN> start_scn_q_;

   public:
    static SCN getLastCommitScn() { return last_commit_scn_.load(); }
    static SCN getRestartScn() { return restart_scn_.load(); }

    static void setRestartScn(const SCN& scn) { restart_scn_ = scn; }
    static void setCommitScn(const SCN& scn) {
      if (last_commit_scn_.load() < scn) last_commit_scn_ = scn;
    }

    static void setRestartScnWhenCommit(SCN& scn) {
      start_scn_q_.erase(scn);
      if (restart_scn_.load() == scn) {
        if (start_scn_q_.empty()) {
          setRestartScn(commit_trans_.begin()->second->start_scn_);
        } else {
          setRestartScn(*(start_scn_q_.begin()));
        }
      }
    }

    static uint32_t removeUncompletedTrans() {
      uint32_t n = 0;
      auto it = xid_map_.begin();
      while (it != xid_map_.end()) {
        if (it->second->start_scn_.empty()) {
          it = xid_map_.erase(it);
          n += 1;
        } else {
          ++it;
        }
      }
      return n;
    }

   private:
    static std::atomic<SCN> last_commit_scn_;
    static std::atomic<SCN> restart_scn_;
  };

  XIDMap::iterator buildTransaction(XIDMap::iterator it);

  typedef std::shared_ptr<Transaction> TransactionPtr;
  typedef std::map<SCN, TransactionPtr>& SCNTranMap;

  void addToTransaction(RecordBufPtr ptr);
  void makeTranRecord(XID xid, RowChangePtr rcp, std::list<Row>& undo,
                      std::list<Row>& redo);

  bool verifyTrans(TransactionPtr trans_ptr);
  Ushort findPk(std::shared_ptr<TabDef> tab_def, const Row& undo,
                OrderedPK& pk);
  std::string colAsStr2(ColumnChangePtr col, std::shared_ptr<TabDef> tab_def);
}
#endif /* ----- #ifndef TRANS_INC  ----- */
