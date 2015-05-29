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
#include "tconvert.h"

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
    stmt_dict_[tab_name]->set_commit(0);
  }

  void SimpleApplier::_apply(RowChangePtr rcp, TabDefPtr tab_def, XID xid,
                             char offset = 0) {
    auto tab_name = tab_def->getTabName();
    otl_stream* this_stream = stmt_dict_[tab_name].get();
    (*this_stream) << std::to_string(xid).c_str();
    (*this_stream) << getOpStr(rcp->op_).c_str();
    if (offset > 0) {
      rcp->scn_.noffset_ += 1;
    }
    (*this_stream) << rcp->scn_.toString().c_str();
    (*this_stream) << epochToTime(rcp->epoch_).c_str();
    switch (rcp->op_) {
      case opcode::kInsert:
      case opcode::kMultiInsert:
        for (auto pk_col : rcp->new_pk_) {
          (*this_stream) << colAsStr2(pk_col, tab_def).c_str();
        }
        break;
      case opcode::kUpdate:
      case opcode::kDelete:
        for (auto pk_col : rcp->old_pk_) {
          (*this_stream) << colAsStr2(pk_col, tab_def).c_str();
        }
        break;
    }
  }

  void SimpleApplier::apply(TransactionPtr tran) {
    try {
      for (auto rc : tran->changes_) {
        if (!rc->completed()) {
          LOG(ERROR) << "Transaction ID " << tran->xid_
                     << " Incompleted Row Change: SCN " << rc->scn_.toStr();
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
            _apply(rc, tab_def, tran->xid_, 1);
            continue;
          }
        }
        _apply(rc, tab_def, tran->xid_);
      }
      conn_.commit();
    } catch (otl_exception& p) {
      if (p.code != 1) throw p;
    }
    Transaction::setTimePointWhenCommit(tran);
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

  ApplierHelper::ApplierHelper(const char* conn_str, uint32_t inst_id)
      : conn_(conn_str),
        inst_id_(inst_id),
        save_progress_stmt_(1,
                            "INSERT INTO stream_progress "
                            " (INST_ID, COMMIT_SCN_MAJOR, COMMIT_SCN_MINOR, "
                            "COMMIT_SUBSCN, COMMIT_OFFSET, COMMIT_EPOCH"
                            " START_SCN_MAJOR, START_SCN_MINOR, START_SUBSCN, "
                            "START_OFFSET, START_EPOCH, CREATION_DATE) "
                            "VALUES (:INST_ID<unsigned>, "
                            ":COMMIT_SCN_MAJOR<unsigned>, "
                            ":COMMIT_SCN_MINOR<unsigned>, "
                            ":COMMIT_SUBSCN<unsigned>, "
                            ":COMMIT_OFFSET<unsigned>, "
                            ":COMMIT_EPOCH<unsigned>, "
                            ":START_SCN_MAJOR<unsigned>, "
                            ":START_SCN_MINOR<unsigned>, "
                            ":START_SUBSCN<unsigned>, "
                            ":START_OFFSET<unsigned>, "
                            ":START_EPOCH<unsigned>, "
                            "SYSDATE)",
                            conn_),
        get_progress_stmt_(1,
                           "SELECT "
                           " COMMIT_SCN_MAJOR, "
                           " COMMIT_SCN_MINOR, "
                           " COMMIT_SUBSCN, "
                           " COMMIT_OFFSET, "
                           " START_SCN_MAJOR, "
                           " START_SCN_MINOR, "
                           " START_SUBSCN, "
                           " START_OFFSET, "
                           " COMMIT_EPOCH, "
                           " START_EPOCH, "
                           " FROM  STREAM_PROGRESS "
                           " WHERE INST_ID = :INST_ID<unsigned> "
                           " AND CREATION_DATE = (SELECT MAX(CREATION_DATE) "
                           " FROM STREAM_PROGRESS) ",
                           conn_) {
    save_progress_stmt_.set_commit(0);
  }

  void ApplierHelper::saveApplyProgress(const TimePoint& commit_tp,
                                        const TimePoint& restart_tp) {
    if (restart_tp.scn_ == SCN(-1)) return;
    save_progress_stmt_ << inst_id_;

    save_progress_stmt_ << commit_tp.scn_.major_;
    save_progress_stmt_ << commit_tp.scn_.minor_;
    save_progress_stmt_ << commit_tp.scn_.subscn_;
    save_progress_stmt_ << commit_tp.scn_.noffset_;
    save_progress_stmt_ << commit_tp.epoch_;

    save_progress_stmt_ << restart_tp.scn_.major_;
    save_progress_stmt_ << restart_tp.scn_.minor_;
    save_progress_stmt_ << restart_tp.scn_.subscn_;
    save_progress_stmt_ << restart_tp.scn_.noffset_;
    save_progress_stmt_ << restart_tp.epoch_;

    conn_.commit();
  }

  ApplyStats ApplierHelper::getApplyStats() {
    get_progress_stmt_ << inst_id_;
    if (get_progress_stmt_.eof()) {
      return ApplyStats();
    }
    TimePoint commit_tp, restart_tp;
    unsigned val;
    int n = 0;
    while (n < 9) {
      get_progress_stmt_ >> val;
      ++n;
      switch (n) {
        case 1:
          commit_tp.scn_.major_ = val;
          break;
        case 2:
          commit_tp.scn_.minor_ = val;
          break;
        case 3:
          commit_tp.scn_.subscn_ = val;
          break;
        case 4:
          commit_tp.scn_.noffset_ = val;
          break;
        case 5:
          restart_tp.scn_.major_ = val;
          break;
        case 6:
          restart_tp.scn_.minor_ = val;
          break;
        case 7:
          restart_tp.scn_.subscn_ = val;
          break;
        case 8:
          restart_tp.scn_.noffset_ = val;
          break;
        case 9:
          commit_tp.epoch_ = val;
          break;
        case 10:
          restart_tp.epoch_ = val;
          break;
      }
    }
    return ApplyStats(restart_tp, commit_tp);
  }
}
