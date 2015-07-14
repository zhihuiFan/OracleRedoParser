#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <vector>
#include <sstream>
#include <string>
#include <unistd.h>

#define OTL_ORA11G_R2
#define OTL_ORA_UTF8
#include "otlv4.h"
#include "applier.h"
#include "trans.h"
#include "util/logger.h"
#include "util/dassert.h"
#include "metadata.h"
#include "stream.h"
#include "tconvert.h"

namespace databus {
  SimpleApplier::SimpleApplier(const char* conn_str,
                               const char* primary_conn_str)
      : conn_str_(conn_str),
        conn_(conn_str),
        table_exist_stmt_(
            1, "SELECT 1 FROM USER_TABLES WHERE TABLE_NAME=upper(:x<char[31]>)",
            conn_) {
    table_exist_stmt_ << "stream_progress";
    if (table_exist_stmt_.eof()) {
      otl_cursor::direct_exec(conn_,
                              "CREATE TABLE STREAM_PROGRESS "
                              "(INST_ID  NUMBER(38) NOT NULL,"
                              "COMMIT_SCN_MAJOR  NUMBER(38) NOT NULL,"
                              "COMMIT_SCN_MINOR  NUMBER(38) NOT NULL,"
                              "COMMIT_SUBSCN  NUMBER(38) NOT NULL,"
                              "COMMIT_OFFSET  NUMBER(38) NOT NULL,"
                              "START_SCN_MAJOR  NUMBER(38) NOT NULL,"
                              "START_SCN_MINOR  NUMBER(38) NOT NULL,"
                              "START_SUBSCN  NUMBER(38) NOT NULL,"
                              "START_OFFSET  NUMBER(38) NOT NULL,"
                              "CREATION_DATE  DATE NOT NULL,"
                              "APPLIED_TIME  DATE NOT NULL,"
                              "RESTART_TIME  DATE NOT NULL,"
                              "COMMIT_EPOCH  NUMBER(38) NOT NULL,"
                              "RESTART_EPOCH  NUMBER(38) NOT NULL)");
      otl_cursor::direct_exec(
          conn_, "CREATE INDEX STREAM_CD ON STREAM_PROGRESS (CREATION_DATE)");
    }
    ApplyStats tp = ApplierHelper::getApplierHelper().getApplyStats();
    if (tp.restart_tp_.empty()) {
      LOG(INFO) << "This is the your first time to run stream for this "
                   "instance, set the current timepoint for you";
      otl_connect primary_conn(primary_conn_str);
      otl_stream current_scn_st(
          1, "select to_char(current_scn) from v$database where 1=:n<int>",
          primary_conn);
      current_scn_st << 1;
      char current_scn[50];
      current_scn_st >> current_scn;
      primary_conn.logoff();
      otl_stream init_tm(
          1,
          "insert into stream_progress "
          " (inst_id,  "
          " commit_scn_major, commit_scn_minor, commit_subscn, commit_offset, "
          " start_scn_major, start_scn_minor, start_subscn, start_offset, "
          " creation_date, applied_time, restart_time, commit_epoch, "
          "restart_epoch) "
          "values(1, 0, 0, 0, 0, "
          "trunc(to_number(:scn<char[50]>)/power(2,32)), "
          "mod(to_number(:scn<char[50]>), power(2,32)), 0, 0, sysdate, "
          "sysdate, sysdate, 0, 0)",
          conn_);
      init_tm << current_scn;
      conn_.commit();
      otl_cursor::direct_exec(conn_, "alter system switch logfile");
      sleep(3);
    }
  }

  void SimpleApplier::addTable(TabDefPtr tab_def, const TabConf& tab_conf,
                               bool force) {
    auto tab_name = tab_def->getTabName();
    if (stmt_dict_.find(tab_name) != stmt_dict_.end() and !force) {
      LOG(WARNING) << " statment for " << tab_name << " exists already";
      return;
    }
    ensureLogTableCreated(tab_def, tab_conf);
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

  void SimpleApplier::ensureLogTableCreated(TabDefPtr tab_def,
                                            const TabConf& tab_conf) {
    table_exist_stmt_ << tab_def->name.c_str();
    if (table_exist_stmt_.eof()) {
      // create the table
      std::stringstream ss;
      ss << "create table " << tab_def->name << " ( "
         << gen_prefix_cols_string() << gen_pk_string(tab_def) << ")";
      if (!tab_conf.tbs_name.empty()) {
        ss << " tablespace " << tab_conf.tbs_name;
      }
      otl_cursor::direct_exec(conn_, ss.str().c_str());

      ss.clear();
      ss.str(std::string());
      ss << "alter table " << tab_def->name << " add primary key(stream_scn)";
      if (!tab_conf.tbs_name.empty()) {
        ss << "using index tablespace " << tab_conf.tbs_name;
      }
      otl_cursor::direct_exec(conn_, ss.str().c_str());
    }
  }

  std::string SimpleApplier::gen_pk_string(TabDefPtr tab_def) {
    std::stringstream ss;
    for (auto col_no : tab_def->pk) {
      auto col_type = tab_def->col_types[col_no];
      ss << tab_def->col_names[col_no] << " " << col_type;
      if (col_type == "NUMBER") {
        if (tab_def->col_len[col_no] != 9999) {
          ss << "(" << tab_def->col_len[col_no];
          if (tab_def->col_scale[col_no] != 9999) {
            ss << "," << tab_def->col_scale[col_no];
          }
          ss << ")";
        }
      } else if (col_type == "VARCHAR2" || col_type == "CHAR") {
        if (tab_def->col_len[col_no] != 9999) {
          ss << "(" << tab_def->col_len[col_no] << ")";
        }
      }
      ss << ",";
    }
    std::string s = ss.str();
    s.pop_back();
    return s;
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
        save_progress_stmt_(
            1,
            "INSERT INTO stream_progress "
            " (INST_ID, COMMIT_SCN_MAJOR, COMMIT_SCN_MINOR, "
            "COMMIT_SUBSCN, COMMIT_OFFSET, APPLIED_TIME, "
            " START_SCN_MAJOR, START_SCN_MINOR, START_SUBSCN, "
            "START_OFFSET, RESTART_TIME,  COMMIT_EPOCH, RESTART_EPOCH, "
            "CREATION_DATE) "
            "VALUES (:INST_ID<unsigned>, "
            ":COMMIT_SCN_MAJOR<unsigned>, "
            ":COMMIT_SCN_MINOR<unsigned>, "
            ":COMMIT_SUBSCN<unsigned>, "
            ":COMMIT_OFFSET<unsigned>, "
            "to_date(:APPLIED_TIME<char[30]>, 'yyyy-mm-dd hh24:mi:ss'), "
            ":START_SCN_MAJOR<unsigned>, "
            ":START_SCN_MINOR<unsigned>, "
            ":START_SUBSCN<unsigned>, "
            ":START_OFFSET<unsigned>, "
            "to_date(:RESTART_TIME<char[30]>, 'yyyy-mm-dd hh24:mi:ss'), "
            ":COMMIT_EPOCH<unsigned>, "
            ":RESTART_EPOCH<unsigned>,"
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
                           " RESTART_EPOCH"
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

    save_progress_stmt_ << (unsigned)commit_tp.scn_.major_;
    save_progress_stmt_ << commit_tp.scn_.minor_;
    save_progress_stmt_ << commit_tp.scn_.subscn_;
    save_progress_stmt_ << commit_tp.scn_.noffset_;
    save_progress_stmt_ << epochToTime(commit_tp.epoch_).c_str();

    save_progress_stmt_ << (unsigned)restart_tp.scn_.major_;
    save_progress_stmt_ << restart_tp.scn_.minor_;
    save_progress_stmt_ << restart_tp.scn_.subscn_;
    save_progress_stmt_ << restart_tp.scn_.noffset_;
    save_progress_stmt_ << epochToTime(restart_tp.epoch_).c_str();
    save_progress_stmt_ << commit_tp.epoch_;
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
    int n = 1;
    while (n < 11) {
      get_progress_stmt_ >> val;
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
      n++;
    }
    return ApplyStats(restart_tp, commit_tp);
  }

  ApplierManager::ApplierManager()
      : curr_applying_seq_(GlobalStream::getGlobalStream().getAppliedSeq()),
        curr_record_(NULL) {}

  void ApplierManager::operator()() {
    while ((curr_record_ = getRecordBufList().pop_front()) != NULL) {
      if (canApply()) {
        applyAllBuf();
      }
      addToTransaction(curr_record_);
      if (curr_record_->seq_ != curr_applying_seq_) {
        GlobalStream::getGlobalStream().setAppliedSeq(++curr_applying_seq_);
      }
    }
  }

  void ApplierManager::applyAllBuf() {
    auto n = Transaction::removeUncompletedTrans();
    if (n > 0)
      LOG(INFO) << "removed " << n << " incompleted transaction in log seq "
                << curr_applying_seq_;
    LOG(DEBUG) << "Build Transaction now" << std::endl;
    auto tran = Transaction::xid_map_.begin();
    while (tran != Transaction::xid_map_.end()) {
      auto it = buildTransaction(tran);
      if (it != Transaction::xid_map_.end()) {
        tran = it;
      } else {
        tran++;
      }
    }
    if (!Transaction::start_scn_q_.empty()) {
      auto it = Transaction::start_scn_q_.begin();
      Transaction::setRestartTimePoint(it->first, it->second);
    }
    LOG(DEBUG) << "Apply Transaction now, Total  "
               << Transaction::commit_trans_.size() << " to apply "
               << std::endl;
    auto commit_tran = Transaction::commit_trans_.begin();
    while (commit_tran != Transaction::commit_trans_.end()) {
      SimpleApplier::getApplier(streamconf->getString("tarConn").c_str(),
                                streamconf->getString("srcConn").c_str())
          .apply(commit_tran->second);
      commit_tran = Transaction::commit_trans_.erase(commit_tran);
    }
  }

  bool ApplierManager::canApply() { return curr_record_->vld_ == 0x05; }
}
