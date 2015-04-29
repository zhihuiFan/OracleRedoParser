#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <vector>
#include <sstream>
#include <string>

#include "applier.h"
#include "trans.h"
#include "util/logger.h"
#include "util/dassert.h"
#include "metadata.h"
#include "stream.h"

namespace databus {
  SimpleApplier::SimpleApplier(const char* conn_str)
      : conn_str_(conn_str), conn_(conn_str) {}

  void SimpleApplier::addTable(TabDefPtr tab_def, bool force) {
    auto tab_name = tab_def->getTabName();
    if (stmt_dict_.find(tab_name) != stmt_dict_.end() and !force) {
      LOG(WARNING) << " statment for " << tab_name << " exists already";
      return;
    }
    auto insert_sql = getInsertStmt(tab_def);
    LOG(INFO) << insert_sql;
    stmt_dict_[tab_name] = std::shared_ptr<otl_stream>(
        new otl_stream(10, insert_sql.c_str(), conn_));
  }

  void SimpleApplier::apply(TransactionPtr tran) {
    int n = 0;
    for (auto rc : tran->changes_) {
      auto pk_data = rc->getPk();
      if (!pk_data.empty()) {
        auto tab_def = getMetadata().getTabDefFromId(rc->object_id_);
        auto tab_name = tab_def->getTabName();
        (*(stmt_dict_[tab_name].get())) << std::to_string(tran->xid_).c_str();
        for (auto& pk_col : pk_data) {
          (*(stmt_dict_[tab_name].get())) << pk_col.c_str();
        }
      }
      n++;
    }
    LOG(INFO) << tran->xid_ << " : " << n;
    conn_.commit();
  }

  std::string SimpleApplier::getInsertStmt(TabDefPtr tab_def) {
    // the statement is order by col_no of pk
    static auto insert_template =
        boost::format("insert into %s(%s) values(%s)");

    auto pre_cnt = prefix_cols.size();
    auto pk_cnt = tab_def->pk.size();
    std::vector<std::string> col_name(pk_cnt + pre_cnt);
    std::vector<std::string> col_value(pk_cnt + pre_cnt);
    char n = 0;
    for (auto p : prefix_cols) {
      col_name[n] = p.first;
      col_value[n++] = p.second;
    }
    for (auto col_no : tab_def->pk) {
      std::stringstream ss;
      if (tab_def->col_types[col_no] == "NUMBER") {
        ss << "TO_NUMBER(:" << tab_def->col_names[col_no] << "<char[127]>)";
      } else if (tab_def->col_types[col_no] == "VARCHAR2" ||
                 tab_def->col_types[col_no] == "CHAR") {
        ss << ":" << tab_def->col_names[col_no] << "<char["
           << tab_def->col_len[col_no] << "]>";
      } else if (tab_def->col_types[col_no] == "DATE") {
        ss << "TO_DATE(:" << tab_def->col_names[col_no]
           << "<char[21]>, 'yyyy-mm-dd hh24:mi:ss')";
      } else {
        LOG(ERROR) << " FIND UNSUPPORT DATA TYPE "
                   << tab_def->col_types[col_no];
        LOG(ERROR) << " ONLY NUMBER/CHAR/VARCHAR2/DATE SUPPORTED SO FAR "
                   << tab_def->col_types[col_no];
        std::exit(100);
      }
      col_name[n] = tab_def->col_names[col_no];
      col_value[n++] = ss.str();
    }
    return (insert_template % tab_def->name % boost::join(col_name, ",") %
            boost::join(col_value, ",")).str();
  }

  ApplierHelper::ApplierHelper(const char* conn_str, const std::string& inst_id)
      : conn_(conn_str),
        inst_id_(inst_id),
        save_progress_stmt_(1,
                            "INSERT INTO stream_progress "
                            " (INST_ID, COMMIT_SCN, START_SCN, CREATION_DATE) "
                            "VALUES (:INST_ID<char[20]>, "
                            "TO_NUMBER(:COMMIT_SCN<char[39]>), "
                            "TO_NUMBER(:START_SCN<char[39]>), SYSDATE)",
                            conn_),
        get_progress_stmt_(1,
                           "SELECT to_char(COMMIT_SCN), to_char(START_SCN) "
                           " FROM  STREAM_PROGRESS "
                           " WHERE INST_ID = :INST_ID<char[20]> "
                           " AND CREATION_DATE = (SELECT MAX(CREATION_DATE) "
                           " FROM STREAM_PROGRESS) ",
                           conn_) {}
}
