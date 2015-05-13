#ifndef TRANS_INC
#define TRANS_INC
#include <map>
#include <set>
#include <vector>
#include <memory>
#include <string>
#include <sstream>
#include <mutex>
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
        : commited_(0),
          xid_(0),
          start_scn_(),
          commit_scn_(),
          ordered(false),
          last_col_no_(0) {}
    XID xid_;
    SCN start_scn_;
    SCN commit_scn_;
    char commited_;  // 2=commited  4=rollbacked
    // orginize changes, for row-chains, row migration
    bool ordered;
    std::set<RowChangePtr, Less> changes_;
    std::string toString() const;
    // for big insert only
    Ushort last_col_no_;

    bool operator<(const Transaction& other) const;
    bool has_rollback() const { return commited_ & 4; }
    bool has_commited() const { return commited_ & 2; }
    bool empty() const { return changes_.empty(); }
    void tidyChanges();

   private:
    void merge(RowChangePtr r);
    bool lastCompleted() const;

   public:
    static XIDMap xid_map_;
    static DBAMap dba_map_;
    static std::map<SCN, std::shared_ptr<Transaction>> commit_trans_;

   public:
    static SCN getLastCommitScn();
    static void setCommitScn(const SCN& scn);
    static SCN getRestartScn();
    static void setRestartScn(const SCN& scn);
    static void apply(std::shared_ptr<Transaction> tran);

   private:
    static std::mutex commit_mutex_;
    static std::mutex restart_mutex_;
    static SCN last_commit_scn_;
    static SCN restart_scn_;
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
