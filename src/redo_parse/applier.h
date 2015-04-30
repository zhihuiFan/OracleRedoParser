#ifndef APPLIER_INC
#define APPLIER_INC
#include <map>
#include <string>
#include <memory>
#include <utility>
#include <list>
#define OTL_ORA11G_R2
#define OTL_ORA_UTF8
#include "otlv4.h"
#include "trans.h"

namespace databus {
  const std::list<std::pair<std::string, std::string>> prefix_cols{
      std::make_pair("XID", "to_number(:stream_xid<char[39]>)"),
      std::make_pair("OP", ":stream_op<char[40]>"),
      std::make_pair(
          "SCN",
          "to_number(:stream_scn<char[29]>, 'XXXXXXXXXXXXXXXXXXXXXXXXXXXX')")};
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
    SimpleApplier(const char* conn_str);
    std::string getInsertStmt(TabDefPtr tab_def);

   private:
    std::string conn_str_;
    otl_connect conn_;
    std::map<std::string, std::shared_ptr<otl_stream>> stmt_dict_;
  };

  struct ApplyStats {
    ApplyStats(const SCN& restart_scn, const SCN& commit_scn)
        : restart_scn_(restart_scn), commit_scn_(commit_scn) {}
    SCN restart_scn_;
    SCN commit_scn_;
  };

  class ApplierHelper {
    // record/get the apply progress into/from database

   public:
    ApplyStats getApplyStats();
    void saveApplyProgress(const SCN& commit_scn, const SCN& restart_scn);
    static ApplierHelper& getApplierHelper(const char* conn_str,
                                           const std::string& inst_id) {
      static ApplierHelper applierHelper(conn_str, inst_id);
      return applierHelper;
    }

   private:
    ApplierHelper(const char* conn_str, const std::string& inst_id);

   private:
    std::string inst_id_;
    otl_connect conn_;
    otl_stream save_progress_stmt_;
    otl_stream get_progress_stmt_;
  };
}
#endif /* ----- #ifndef APPLIER_INC  ----- */
