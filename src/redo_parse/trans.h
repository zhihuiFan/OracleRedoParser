#ifndef TRANS_INC
#define TRANS_INC
#include <map>
#include <set>
#include <vector>
#include <memory>
#include <string>
#include <sstream>
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
    std::string toString(bool scn = false) const;
    std::string pkToString() const;
    std::vector<std::string> getPk();

    SCN scn_;
    uint32_t object_id_;
    Ushort op_;
    Ushort uflag_;
    Ushort iflag_;
    OrderedPK pk_;
    Row new_data_;
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
    static XIDMap xid_map_;
    static DBAMap dba_map_;
    static std::map<SCN, std::shared_ptr<Transaction>> commit_trans_;
    static SCN last_commit_scn_;

    bool operator<(const Transaction& other) const;
    bool has_rollback() const { return commited_ & 4; }
    bool has_commited() const { return commited_ & 2; }
    bool empty() const { return changes_.empty(); }
    void tidyChanges();
    static void apply(std::shared_ptr<Transaction> tran);
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
  void makeTranRecord(XID xid, uint32_t object_id, Ushort op,
                      std::list<Row>& undo, std::list<Row>& redo,
                      const SCN& scn, Ushort uflag, Ushort iflag);

  bool verifyTrans(TransactionPtr trans_ptr);
  Ushort findPk(std::shared_ptr<TabDef> tab_def, const Row& undo,
                OrderedPK& pk);
}
#endif /* ----- #ifndef TRANS_INC  ----- */
