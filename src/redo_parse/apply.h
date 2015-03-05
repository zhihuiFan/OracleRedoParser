#include <map>
#include <occi.h>
#include <memory>
#include <bitset>

typedef std::bitset<256> Key;
struct Comparer {
  bool operator()(const Key &l, const Key &r) {
    auto n = l.size();
    while (n > 0) {
      if (l[n - 1] == r[n - 1])
        n--;
      else
        return l[n - 1] < r[n - 1];
    }
  }
};

class Applier {
  using namespace oracle::occi;

 public:
  Applier();
  void applyOp(std::shared_ptr<RowChange> sp);

 private:
  class TableOp {
   public:
    TableOp(Applier *applier, uint32_t object_id) {}
    void applyOp(std::shared_ptr<RowChange> sp);
    // when DDL happens, we may invalidate these cached statement AND TabDef
    void resetAllCaches();

   private:
    Key getColKey4Update(std::shared_ptr<RowChange> sp);
    void runStatement(std::shared_ptr<Statement> stmt_ptr, const Row &new_data);

   private:
    // cached oracle statement for Object_id
    // map key to Oracle Statement
    // For insert, key in cached_stmts equals to the number of cols inserted
    // if insert into t(c1, c2) then key = 2.  so uint32_t can support 2^32 -1
    // cols
    // For update, it uses 1 bit to represent a col
    // update t set col1=a, col3=b, col8=8 where pk ..
    // key = 0b10000101, Key is defined as bitset<256>, so it can support 256
    // cols
    // TODO: add 256 check on MetadataManager part, I think 256 should be enough
    // TODO: need to clear up this statement, or else, it is a memory leak,
    //       find out a LRU cached algrithom in c++
    //       check https://patrickaudley.com/code/project/lrucache for reference
    // TODO: consider if need thread-safe
    std::map<uint32_t, std::shared_ptr<Statement> > cached_insert_stmts_;
    std::map<Key, std::shared_ptr<Statement> > cached_update_stmts_;
    std::shared_ptr<Statement> delete_stmt_;
    TabDefPtr tab_def_;
    uint32_t object_id_;
    Applier *applier_;
  };

  // map object_id to TableOp
  std::map<uint32_t, std::shared_ptr<TableOp> > table_ops_;

  // db connections
  Environment *env_;
  Connection *conn_;

  // we may add some apply counter here, like how many what kind of changes we
  // have applied
  // counter def...
};
