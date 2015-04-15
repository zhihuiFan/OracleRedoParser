#ifndef TRANS_INC
#define TRANS_INC
#include <map>
#include <set>
#include <memory>
#include <string>
#include <sstream>
#include "logical_elems.h"
#include "opcode.h"
#include "util/dtypes.h"
#include "logical_elems.h"

namespace databus {

  struct RowChange {
    RowChange();
    RowChange(SCN& scn, uint32_t obj_id, Ushort op, Ushort uflag, Ushort iflag,
              Row& undo, Row& redo);
    bool operator<(const RowChange& other) const { return scn_ < other.scn_; }
    std::string toString(bool scn = false) const;

    SCN scn_;
    uint32_t object_id_;
    Ushort op_;
    Ushort uflag_;
    Ushort iflag_;
    Row pk_;
    Row new_data_;

    bool isNormal() const {
      return op_ == opcode::kDelete || op_ == opcode::kMultiInsert ||
             (op_ == opcode::kInsert && iflag_ == 0x2c);
    }
  };

  typedef std::shared_ptr<RowChange> RowChangePtr;

  struct Transaction;
  typedef std::map<XID, std::shared_ptr<Transaction>> XIDMap;
  typedef std::map<DBA, USN> DBAMap;

  struct Transaction {
    Transaction()
        : commited_(0), xid_(0), start_scn_(), commit_scn_(), ordered(false) {}
    XID xid_;
    SCN start_scn_;
    SCN commit_scn_;
    char commited_;  // 2=commited  4=rollbacked
    // orginize changes, for row-chains, row migration
    bool ordered;
    std::multiset<RowChangePtr> changes_;
    std::string toString() const;
    static XIDMap xid_map_;
    static DBAMap dba_map_;
    static std::map<SCN, std::shared_ptr<Transaction>> commit_trans_;

    bool operator<(const Transaction& other) const;
    bool has_rollback() const { return commited_ & 4; }
    bool has_commited() const { return commited_ & 2; }
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
    void buildTransaction();
    void tidyChanges();
  };

  typedef std::shared_ptr<Transaction> TransactionPtr;
  typedef std::map<SCN, TransactionPtr>& SCNTranMap;

  void addToTransaction(RecordBufPtr ptr);
  void makeTranRecord(XID xid, uint32_t object_id, Ushort op,
                      std::list<Row>& undo, std::list<Row>& redo,
                      const SCN& scn, Ushort uflag, Ushort iflag);

  bool verifyTrans(TransactionPtr trans_ptr);

  void dump(TransactionPtr trans);
}
#endif /* ----- #ifndef TRANS_INC  ----- */
