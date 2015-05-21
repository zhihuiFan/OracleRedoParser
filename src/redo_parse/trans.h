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
    Transaction()
        : commited_(0), xid_(0), start_scn_(), commit_scn_(), last_col_no_(0) {}
    XID xid_;
    SCN start_scn_;
    SCN commit_scn_;
    char commited_;  // 2=commited  4=rollbacked
    // orginize changes, for row-chains, row migration
    std::set<RowChangePtr, Less> changes_;
    std::string toString() const;
    // for big insert only
    Ushort last_col_no_;

    bool operator<(const Transaction& other) const;
    bool has_rollback() const { return commited_ & 4; }
    bool has_commited() const { return commited_ & 2; }
    bool empty() const { return changes_.empty(); }
    void tidyChanges();

    static XIDMap xid_map_;
    static std::map<SCN, std::shared_ptr<Transaction>> commit_trans_;

   private:
    void merge(RowChangePtr r);
    bool lastCompleted() const;

   public:
    static DBAMap dba_map_;

   public:
    static SCN getLastCommitScn() { return last_commit_scn_.load(); }
    static SCN getRestartScn() { return restart_scn_.load(); }
    static char getPhase() { return phase_.load(); }

    static void setRestartScn(const SCN& scn);
    static void setCommitScn(const SCN& scn);

    static void setPhase(char c) { phase_ = c; }

    // used when phase 2
    static void addMinStartScnInCommitQ(SCN& scn) {
      start_scn_in_commit_q_.insert(scn);
    }

    // used when phase 3
    static void setMinStartScnInCommitQ(SCN& scn) {
      start_scn_in_commit_q_.remove(scn);
      if (min_start_scn_in_commit_q_.load() == scn) {
        min_start_scn_in_commit_q_ = *(start_scn_in_commit_q_.begin())
      }
    }

   private:
    static std::atomic<SCN> last_commit_scn_;
    static std::atomic<SCN> restart_scn_;
    // 1 means reading recrod buf from logfile to xid_trans_
    // 2 means applying the committed transaction
    static std::atomic<char> phase_;
    // we modify it when phase 2, and change it when in phase 3.
    // we modify min_start_scn_in_commit_q_ according when a transaction is
    // applied
    // So we don't need it to be thread-safe, at least for release 0.1 and 0.2
    static std::set<SCN> start_scn_in_commit_q_;
  };
  // if this transaction is commit,
  //      build applyable changes
  //      move_from_xid_map
  //      add_to_scn_map
  // else if rollbacked:
  //      move_from_xid_map
  // else:  unknown so far
  //      pass
  // about applyable changes
  //      see https://jirap.corp.ebay.com/browse/DBISTREA-19
  // Note: buildTransaction can be call only when one of the following
  // sisutaion is true
  // 1. you have read all the changes in the whole archive log into changes_
  // 2. you have read all the changes in the online log before the flush
  // marker
  // Return value:
  // 0  the transction doesn't rollback or commit, leave it in transaction
  // queue
  // 1  the transaction is rollbacked, remove it from transaction queue
  // 2  the transaction is committed, remove it from transaction, add it to
  // applyable transaction queue
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
