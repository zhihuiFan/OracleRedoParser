#ifndef APPLIER_INC
#define APPLIER_INC
#include <map>
#include <string>
#include <memory>
#include "otlv4.h"
#include "trans.h"

namespace databus {
  const std::map<std::string, std::string> prefix_cols{
      {"OP", ":_stream_op<char[40]>"}};
  class TabDef;
  typedef std::shared_ptr<TabDef> TabDefPtr;
  class SimpleApplier {
    // Only recover PKs
   public:
    // add table statement into stmt_dict_,
    // if false == true, it will overwrite the the previous data
    void addTable(TabDefPtr tab_def, bool force = false);
    void apply(TransactionPtr tran_ptr);
    static SimpleApplier& getApplier(const char* conn_str) {
      static SimpleApplier applier(conn_str);
      return applier;
    }

   private:
    SimpleApplier(const char* conn_str)
        : conn_str_(conn_str), conn_(conn_str) {}
    std::string getInsertStmt(TabDefPtr tab_def);

   private:
    std::string conn_str_;
    otl_connect conn_;
    std::map<std::string, std::shared_ptr<otl_stream>> stmt_dict_;
  };
}
#endif /* ----- #ifndef APPLIER_INC  ----- */
