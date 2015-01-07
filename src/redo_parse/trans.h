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

  enum class Op { NA, INSERT, UPDATE, DELETE, MINSERT };

  struct RowChange {
    RowChange();
    bool operator<(const RowChange& other) const { return scn_ < other.scn_; }
    std::string toString(bool scn = false) const;

    SCN scn_;
    Op op_;
    uint32_t object_id_;
    Row pk_;
    Row new_data_;
  };

  // const std::map<Op, const char*> opmap{{Op::INSERT, " INSERT INTO "},
  const std::map<Op, const char*> opmap{{Op::INSERT, "INSERT INTO "},
                                        {Op::UPDATE, "UPDATE "},
                                        {Op::DELETE, "DELETE FROM "}};

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
  void makeTranRecord(XID xid, uint32_t object_id, Op op, std::list<Row>& undo,
                      std::list<Row>& redo, const SCN& scn);

  bool verifyTrans(TransactionPtr trans_ptr);

  void dump(TransactionPtr trans);
}
#endif /* ----- #ifndef TRANS_INC  ----- */
