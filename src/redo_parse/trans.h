#ifndef TRANS_INC
#define TRANS_INC
#include <map>
#include <set>
#include <memory>
#include <string>
#include <sstream>
#include "logical_elems.h"
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
  };

  typedef std::shared_ptr<RowChange> RowChangePtr;

  struct Transaction;
  typedef std::map<XID, std::shared_ptr<Transaction>>& XIDMap;
  typedef std::map<DBA, USN>& DBAMap;

  struct Transaction {
    Transaction() : commited_(0), xid_(0) {}
    XID xid_;
    SCN start_scn_;
    SCN commit_scn_;
    char commited_;  // 2=commited  4=rollbacked
    std::multiset<RowChangePtr> changes_;
    std::string toString() const;
    bool operator<(const Transaction& other) const;
    static XIDMap getXIDMap();
    static DBAMap getDBAMap();  // Undo Segment Num
  };

  typedef std::shared_ptr<Transaction> TransactionPtr;

  void buildTransaction(RecordBufPtr ptr);
  void makeTranRecord(XID xid, uint32_t object_id, Ushort op,
                      std::list<Row>& undo, std::list<Row>& redo,
                      const SCN& scn, Ushort uflag, Ushort iflag);

  bool verifyTrans(TransactionPtr trans_ptr);

  void dump(TransactionPtr trans);
}
#endif /* ----- #ifndef TRANS_INC  ----- */
