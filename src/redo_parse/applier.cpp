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
        new otl_stream(1, insert_sql.c_str(), conn_));
  }

  void SimpleApplier::_apply(RowChangePtr rcp, TabDefPtr tab_def, XID xid) {
    auto tab_name = tab_def->getTabName();
    (*(stmt_dict_[tab_name].get())) << std::to_string(xid).c_str();
    (*(stmt_dict_[tab_name].get())) << getOpStr(rcp->op_).c_str();
    (*(stmt_dict_[tab_name].get())) << rcp->scn_.toString().c_str();
    switch (rcp->op_) {
      case opcode::kInsert:
      case opcode::kMultiInsert:
        for (auto pk_col : rcp->new_pk_) {
          (*(stmt_dict_[tab_name].get())) << colAsStr2(pk_col, tab_def).c_str();
        }
        break;
      case opcode::kUpdate:
      case opcode::kDelete:
        for (auto pk_col : rcp->old_pk_) {
          (*(stmt_dict_[tab_name].get())) << colAsStr2(pk_col, tab_def).c_str();
        }
        break;
    }
  }

  void SimpleApplier::apply(TransactionPtr tran) {
    for (auto rc : tran->changes_) {
      if (!rc->completed()) {
        LOG(ERROR) << "Incompleted Row Change: SCN " << rc->scn_.toStr();
        std::exit(21);
      }

      if (rc->op_ == opcode::kRowChain || rc->op_ == opcode::kLmn) {
        rc->op_ = opcode::kUpdate;
      }

      auto tab_def = getMetadata().getTabDefFromId(rc->object_id_);
      auto tab_name = tab_def->getTabName();
      if (rc->op_ == opcode::kUpdate) {
        bool same = true;
        for (auto c : rc->old_pk_) {
          auto ret = rc->new_pk_.insert(c);
          if (!ret.second) {
            if (colAsStr2(c, tab_def)
                    .compare(colAsStr2(*(ret.first), tab_def)) != 0) {
              same = false;
            }
          }
        }
        if (!same) {
          rc->op_ = opcode::kDelete;
          _apply(rc, tab_def, tran->xid_);
          rc->op_ = opcode::kInsert;
          _apply(rc, tab_def, tran->xid_);
          n++;
          continue;
        }
      }
      _apply(rc, tab_def, tran->xid_);
    }
    Transaction::setCommitScn(tran->commit_scn_);
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
           << tab_def->col_len[col_no] + 1 << "]>";
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
                            " (INST_ID, COMMIT_SCN_MAJOR, COMMIT_SCN_MINOR, "
                            "COMMIT_SUBSCN, COMMIT_OFFSET, "
                            " START_SCN_MAJOR, START_SCN_MINTOR, START_SUBSCN, "
                            "START_OFFSET, CREATION_DATE) "
                            "VALUES (:INST_ID<char[20]>, "
                            "TO_NUMBER(:COMMIT_SCN_MAJOR<char[39]>), "
                            "TO_NUMBER(:COMMIT_SCN_MINOR<char[39]>), "
                            "TO_NUMBER(:COMMIT_SUBSCN<char[39]>), "
                            "TO_NUMBER(:COMMIT_OFFSET<char[39]>), "
                            "TO_NUMBER(:START_SCN_MAJOR<char[39]>), "
                            "TO_NUMBER(:START_SCN_MAJOR<char[39]>), "
                            "TO_NUMBER(:START_SCN_MAJOR<char[39]>), "
                            "TO_NUMBER(:START_SCN_MAJOR<char[39]>), "
                            "SYSDATE)",
                            conn_),
        get_progress_stmt_(1,
                           "SELECT "
                           " to_char(COMMIT_SCN_MAJOR), "
                           " to_char(COMMIT_SCN_MINOR), "
                           " to_char(COMMIT_SUBSCN), "
                           " to_char(COMMIT_OFFSET), "
                           " to_char(START_SCN_MAJOR), "
                           " to_char(START_SCN_MINOR), "
                           " to_char(START_SUBSCN), "
                           " to_char(START_OFFSET) "
                           " FROM  STREAM_PROGRESS "
                           " WHERE INST_ID = :INST_ID<char[20]> "
                           " AND CREATION_DATE = (SELECT MAX(CREATION_DATE) "
                           " FROM STREAM_PROGRESS) ",
                           conn_) {}

  void ApplierHelper::saveApplyProgress(const SCN& commit_scn,
                                        const SCN& restart_scn) {
    save_progress_stmt_ << inst_id_.c_str()
                        << std::to_string(commit_scn.major_).c_str()
                        << std::to_string(commit_scn.minor_).c_str()
                        << std::to_string(commit_scn.subscn_).c_str()
                        << std::to_string(commit_scn.noffset_).c_str()
                        << std::to_string(restart_scn.major_).c_str()
                        << std::to_string(restart_scn.minor_).c_str()
                        << std::to_string(restart_scn.subscn_).c_str()
                        << std::to_string(restart_scn.noffset_).c_str();
    conn_.commit();
  }

  ApplyStats ApplierHelper::getApplyStats() {
    get_progress_stmt_ << inst_id_.c_str();
    if (get_progress_stmt_.eof()) {
      return ApplyStats(SCN(), SCN());
    }
    SCN commit_scn, restart_scn;
    unsigned char buf[39];
    int n = 0;
    while (n < 9) {
      get_progress_stmt_ >> buf;
      ++n;
      switch (n) {
        case 1:
          commit_scn.major_ = std::stoi(std::string((char*)buf));
          break;
        case 2:
          commit_scn.minor_ = std::stoi(std::string((char*)buf));
          break;
        case 3:
          commit_scn.subscn_ = std::stoi(std::string((char*)buf));
          break;
        case 4:
          commit_scn.noffset_ = std::stoi(std::string((char*)buf)) + 1;
          break;
        case 5:
          restart_scn.major_ = std::stoi(std::string((char*)buf));
          break;
        case 6:
          restart_scn.minor_ = std::stoi(std::string((char*)buf));
          break;
        case 7:
          restart_scn.subscn_ = std::stoi(std::string((char*)buf));
          break;
        case 8:
          restart_scn.noffset_ = std::stoi(std::string((char*)buf)) + 1;
          break;
      }
    }
    return ApplyStats(restart_scn, commit_scn);
  }

  void applyMonitor() {
    ApplierHelper& helper = ApplierHelper::getApplierHelper(
        streamconf->getString("tarConn").c_str(),
        streamconf->getString("instId").c_str());
  }
}
