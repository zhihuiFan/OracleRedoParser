#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <vector>
#include <sstream>

#include "applier.h"
#include "trans.h"
#include "util/logger.h"
#include "util/dassert.h"
#include "otlv4.h"
#include "metadata.h"

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
    LOG(DEBUG) << insert_sql;
    stmt_dict_[tab_name] =
        std::shared_ptr<otl_stream>(new otl_stream(10, insert_sql, conn_));
  }

  void SimpleApplier::apply(TransactionPtr tran) {
    for (auto& rc : tran->changes_) {
      auto pk_data = rc->getPk();
      if (!data.empty()) {
        auto tab_def = getMetadata().getTabDefFromId(c->object_id_);
        auto tab_name = tab_def->getTabName();
        for (auto& pk_col : pk_data) {
          (*(stmt_dict_[tab_name].get())) << pk_col.c_str();
        }
      }
    }
    conn_->commit();
  }

  std::string SimpleApplier::getInsertStmt(TabDefPtr tab_def) {
    // the statement is order by col_no of pk
    auto insert_template = boost::format("insert into %s(%s) values(%s)");

    auto pre_cnt = prefix_cols.size();
    auto pk_cnt = tab_def->pk.size();
    std::vector<std::string> col_name(pk_cnt + pre_cnt);
    std::vector<std::string> col_value(pk_cnt + pre_cnt);
    char n = 0;
    for (auto p : prefix_cols) {
      col_name[n] = p.first;
      col_value[n++] = p.second;
    }
    std::stringstream ss;
    for (auto col_no : tab_def->pk) {
      if (tab_def->col_types[col_no] == "NUMBER") {
        ss << "TO_NUMBER(:" << col_name[n] << "<char[39]>)";
      } else if (tab_def->col_types[col_no] == "VARCHAR2" ||
                 tab_def->col_types[col_no] == "CHAR") {
        ss << ":" << col_name[n] << "<char[" << tab_def->col_len[n] << "]>";
      } else if (tab_def->col_types[col_no] != "DATE") {
        ss << "TO_DATE(:" << col_name[n] << "<char[21]>";
      } else {
        LOG(ERROR) << " FIND UNSUPPORT DATA TYPE "
                   << tab_def->col_types[col_no];
        LOG(ERROR) << " ONLY NUMBER/CHAR/VARCHAR2/DATE SUPPORTED SO FAR "
                   << tab_def->col_types[col_no];
        std::exit(100);
      }
    }
    col_name[n] = tab_def->col_names[col_no];
    col_value[n++] = ss.str();
    ss.clear();
    return insert_template % tab_def->getTabName() %
           boost::join(col_name, ",") % boost::join(col_value, ",");
  }
}
