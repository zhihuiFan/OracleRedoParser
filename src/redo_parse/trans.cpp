#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <map>
#include <list>
#include <vector>
#include <sstream>
#include "trans.h"
#include "opcode.h"
#include "opcode_ops.h"
#include "metadata.h"
#include "tconvert.h"
#include "util/logger.h"
#include "util/dassert.h"
#include "stream.h"

namespace databus {
  bool Transaction::operator<(const Transaction& other) const {
    return commit_scn_ < other.commit_scn_;
  }

  DBAMap Transaction::dba_map_;
  XIDMap Transaction::xid_map_;
  std::map<SCN, TransactionPtr> Transaction::commit_trans_;

  XIDMap::iterator buildTransaction(XIDMap::iterator it) {
    if (it->second->has_rollback()) {
      return Transaction::xid_map_.erase(it);
    } else if (it->second->has_commited()) {
      it->second->tidyChanges();
      Transaction::commit_trans_[it->second->commit_scn_] =
          Transaction::xid_map_[it->second->xid_];
      return Transaction::xid_map_.erase(it);
    }
    return Transaction::xid_map_.end();
  }

  void Transaction::tidyChanges() {
    auto temp_changes_ = std::move(changes_);
    for (auto& rc : temp_changes_) {
      if (rc->op_ == opcode::kDelete || rc->op_ == opcode::kMultiInsert ||
          rc->op_ == opcode::kUpdate) {
        changes_.insert(rc);
      } else if (rc->op_ == opcode::kInsert) {
        if (rc->iflag_ == 0x2c) {
          // normal insert
          util::dassert("normal iflag error 0x2c", last_col_no_ == 0);
          changes_.insert(rc);
        } else if (rc->iflag_ == 0x04) {
          util::dassert("row chain iflag error 0x04", last_col_no_ == 0);
          changes_.insert(rc);
          last_col_no_ = rc->new_data_.size();
        } else if (rc->iflag_ == 0x00 || rc->iflag_ == 0x28) {
          RowChangePtr lastRowPtr = *changes_.rbegin();
          util::dassert(
              "row chain middle error",
              last_col_no_ > 0 && last_col_no_ == lastRowPtr->new_data_.size());
          Ushort newElemLen = rc->new_data_.size();
          for (ColumnChangePtr c : lastRowPtr->new_data_) {
            c->col_id_ += newElemLen;
          }
          for (auto c : rc->new_data_) {
            for (auto cl : lastRowPtr->new_data_) {
              dassert("col_no_error", cl->col_id_ != c->col_id_);
            }
          }
          lastRowPtr->new_data_.splice(lastRowPtr->new_data_.end(),
                                       rc->new_data_);
          if (rc->iflag_ == 0x00) {
            last_col_no_ += newElemLen;
          } else {
            last_col_no_ = 0;
          }
        }
      } else if (rc->op_ == opcode::kRowChain) {
        // A big insert before 11.6 may be imcompleted
        // Check https://jirap.corp.ebay.com/browse/DBISTREA-21
        last_col_no_ = 0;
        dassert("Row Chain Exception 1", !changes_.empty());
        auto it = changes_.end();
        --it;
        dassert("Row Chain Exception 2",
                (*it)->op_ == opcode::kInsert &&
                    (*it)->object_id_ == rc->object_id_);
        changes_.erase(it);
        changes_.insert(rc);
      }
    }
  }

  void Transaction::apply(TransactionPtr tran) {
    // will write this part later
    LOG(INFO) << "Apply Transaction " << tran->xid_ << std::endl;
  }

  std::string Transaction::toString() const {
    std::stringstream ss;
    ss << std::endl << "XID " << std::hex << xid_
       << " start_scn : " << start_scn_.toStr()
       << " commit_scn: " << commit_scn_.toStr()
       << " commited  : " << (int)commited_ << std::endl;
    for (auto& rc : changes_) {
      ss << rc->pkToString() << std::endl;
    }
    return ss.str();
  }

  static std::string colAsStr(ColumnChangePtr col, TabDefPtr tab_def,
                              char seperator = ':') {
    std::stringstream ss;
    ss << tab_def->col_names[col->col_id_ + 1] << seperator
       << convert(col->content_, tab_def->col_types[col->col_id_ + 1],
                  col->len_);
    return ss.str();
  }

  static int findPk(std::shared_ptr<TabDef> table_def, const Row& undo,
                    Row& pk) {
    for (auto col : undo) {
      if (col->len_ > 0 &&
          table_def->pk.find(col->col_id_ + 1) != table_def->pk.end()) {
        pk.push_back(col);
      }
    }
    int npk = pk.size();
    if (npk == table_def->pk.size()) {
      return npk;
    } else {
      LOG(WARNING) << "Number of PK Mismatched\n Object Name "
                   << table_def->owner << "." << table_def->name
                   << " Num of PK " << table_def->pk.size()
                   << " No. of PK in Redo " << pk.size()
                   << " This probably a Row Megration 11.2 \n";
      return -1;
    }
  }

  RowChange::RowChange()
      : scn_(),
        object_id_(0),
        op_(),
        uflag_(0),
        iflag_(0),
        pk_{},
        new_data_{} {}
  RowChange::RowChange(SCN& scn, uint32_t obj_id, Ushort op, Ushort uflag,
                       Ushort iflag, Row& undo, Row& redo)
      : scn_(scn), object_id_(obj_id), op_(op), uflag_(uflag), iflag_(iflag) {
    auto table_def = getMetadata().getTabDefFromId(object_id_);
    switch (op_) {
      case opcode::kDelete: {
        Row pk;
        int ret = findPk(table_def, undo, pk);
        if (ret > 0) {
          pk_ = std::move(pk);
        }
      } break;
      case opcode::kInsert: {
        for (auto col : redo) {
          if (col->len_ > 0) {
            new_data_.push_back(col);
          }
        }
      } break;
      case opcode::kUpdate: {
      }
    }
  }
  std::string RowChange::toString(bool scn) const {
    static auto format_insert = boost::format("insert into %s(%s) values(%s)");
    static auto format_update = boost::format("update %s set %s where %s");
    static auto format_delete = boost::format("delete from %s where %s");
    static auto not_empty = [](const std::string& s)
                                -> bool { return !s.empty(); };

    TabDefPtr tab_def = getMetadata().getTabDefFromId(object_id_);
    std::stringstream ss;
    ss << tab_def->owner << "." << tab_def->name;
    std::string table_name = ss.str();

    switch (op_) {
      case opcode::kInsert:
      case opcode::kMultiInsert: {
        std::vector<std::string> new_data(new_data_.size());
        std::vector<std::string> col_names(new_data_.size());
        int i = 0;
        for (auto col : new_data_) {
          new_data[i] = std::move(convert(
              col->content_, tab_def->col_types[col->col_id_ + 1], col->len_));
          col_names[i++] = tab_def->col_names[col->col_id_ + 1];
        }

        if (scn) {
          return scn_.toStr() + " " +
                 (format_insert % table_name % boost::join(col_names, ", ") %
                  boost::join_if(new_data, ", ", not_empty)).str();
        } else {
          return (format_insert % table_name % boost::join(col_names, ", ") %
                  boost::join_if(new_data, ", ", not_empty)).str();
        }

      } break;
      case opcode::kUpdate: {
        std::vector<std::string> new_data(new_data_.size());
        std::vector<std::string> where_conds(new_data_.size());
        for (auto col : new_data_) {
          new_data.push_back(std::move(colAsStr(col, tab_def, '=')));
        }
        for (auto col : pk_) {
          where_conds.push_back(std::move(colAsStr(col, tab_def, '=')));
        }
        if (scn) {
          return scn_.toStr() + " " +
                 (format_update % table_name %
                  boost::join_if(new_data, ", ", not_empty) %
                  boost::join_if(where_conds, ", ", not_empty)).str();
        } else {
          return (format_update % table_name %
                  boost::join_if(new_data, ", ", not_empty) %
                  boost::join_if(where_conds, ", ", not_empty)).str();
        }
      } break;
      case opcode::kDelete: {
        std::vector<std::string> where_conds(new_data_.size());
        for (auto col : pk_) {
          where_conds.push_back(std::move(colAsStr(col, tab_def, '=')));
        }
        if (scn) {
          return scn_.toStr() + " " +
                 (format_delete % table_name %
                  boost::join_if(where_conds, ", ", not_empty)).str();
        } else {
          return (format_delete % table_name %
                  boost::join_if(where_conds, ", ", not_empty)).str();
        }
      } break;
      default:
        LOG(ERROR) << "Unknown Op " << (int)op_ << std::endl;
    }
    return "";
  }

  std::string RowChange::pkToString() const {
    std::stringstream ss;
    TabDefPtr tab_def = getMetadata().getTabDefFromId(object_id_);
    ss << tab_def->owner << "." << tab_def->name << "  " << getOpStr(op_);
    // for update, delete, row migration, we store pk into pk_ already
    if (op_ == opcode::kInsert || op_ == opcode::kMultiInsert) {
      Row pk;
      if (findPk(tab_def, new_data_, pk) == -1) {
        return std::string("Probably Row Megration");
      }
      for (ColumnChangePtr c : pk) {
        ss << colAsStr(c, tab_def);
      }
    } else {
      for (ColumnChangePtr c : pk_) {
        ss << colAsStr(c, tab_def);
      }
    }
    return ss.str();
  }

  void addToTransaction(RecordBufPtr record) {
    DBA dba = 0;
    XID xid = 0;
    std::list<Row> undo, redo;
    uint32_t object_id;
    SCN record_scn = record->scn();
    SCN trans_start_scn;
    OpCodeSupplemental* opsup = NULL;
    Ushort uflag = 0;
    Uchar iflag = 0;
    Ushort op = 0;
    for (auto change : record->change_vectors) {
      switch (change->opCode()) {
        case opcode::kBeginTrans:
          dba = change->dba();
          {
            if (((OpCode0502*)change->part(1))->sqn_ > 0) {
              // sqn_ == 0 is not a start of transaction
              trans_start_scn = record_scn;
            }
          }
          break;
        case opcode::kUndo:
          xid = Ops0501::getXID(change);
          if (xid == 0) return;
          object_id = Ops0501::getObjId(change);
          if (dba > 0) {
            Transaction::dba_map_[dba] =
                ((OpCode0501*)change->part(1))->xid_high_;
            dba = 0;
            if (!trans_start_scn.empty()) {
              auto& xidmap = Transaction::xid_map_;
              auto it = xidmap.find(xid);
              if (it != xidmap.end()) {
                LOG(ERROR) << "I think Transaction (xid" << xid
                           << ") just start here offset: " << record->offset()
                           << " but it was started before \n"
                           << "Dump Info\n" << it->second->toString()
                           << std::endl;
                return;
              }
              xidmap[xid] = TransactionPtr(new Transaction());
              xidmap[xid]->start_scn_ = record_scn;
              xidmap[xid]->xid_ = xid;
            }
          }
          {
            if (getMetadata().getTabDefFromId(object_id, false) == NULL) {
              // we don't care about this object id for version 0.1
              return;
            }
            undo = Ops0501::makeUpUndo(change, uflag, opsup);
            // opsup only set when  irp->xtype_ & 0x20
            // LOG(INFO) << "Sup start_col_offset " << record->offset() << ":"
            //      << opsup->start_column_ << ":" << opsup->start_column2_
            //    << std::endl;
          }
          break;
        case opcode::kUpdate:
        case opcode::kInsert:
        case opcode::kMultiInsert:
          op = change->opCode();
          redo = OpsDML::makeUpRedoCols(change, iflag);
          break;
        case opcode::kDelete:
        case opcode::kRowChain:
          op = change->opCode();
          break;
        case opcode::kCommit:
          dba = change->dba();
          {
            auto it = Transaction::dba_map_.find(dba);
            if (it == Transaction::dba_map_.end()) {
              LOG(WARNING) << "found dba " << dba
                           << " in commit , but unknow when "
                              "this transaction is started" << std::endl;
              return;
            }
            OpCode0504_ucm* ucm = (OpCode0504_ucm*)(change->part(1));
            XID ixid =
                (((XID)it->second) << (sizeof(Ushort) + sizeof(uint32_t)) * 8) |
                (((XID)ucm->slt_) << sizeof(uint32_t) * 8) | ucm->sqn_;
            auto xidit = Transaction::xid_map_.find(ixid);
            if (xidit == Transaction::xid_map_.end()) {
              LOG(WARNING) << "found xid " << dba
                           << " in commit , but unknow when "
                              "this transaction is started" << std::endl;
              return;
            }
            xidit->second->commit_scn_ = record_scn;
            xidit->second->commited_ = ucm->flg_;
          }
      }  // end switch
    }
    if (op && xid != 0)
      makeTranRecord(xid, object_id, op, undo, redo, record_scn, uflag, iflag);
  }

  void makeTranRecord(XID xid, uint32_t object_id, Ushort op,
                      std::list<Row>& undos, std::list<Row>& redos,
                      const SCN& scn, Ushort uflag, Ushort iflag) {
    XIDMap& xidmap = Transaction::xid_map_;
    auto transit = xidmap.find(xid);
    if (transit == xidmap.end()) {
      LOG(ERROR) << "XID " << xid
                 << " info was missed when I want to add a change to it"
                 << std::endl;
      return;
    }
    auto table_def = getMetadata().getTabDefFromId(object_id);
    if (table_def == NULL) {
      LOG(DEBUG) << "Can't get table definition for object_id " << object_id
                 << " ignore this change " << std::endl;
      return;
    }
    switch (op) {
      case opcode::kDelete:
      case opcode::kRowChain: {
        for (auto row : undos) {
          RowChangePtr rcp(new RowChange());
          Row pk;
          int ret = findPk(table_def, row, pk);
          if (ret > 0) {
            rcp->pk_ = std::move(pk);
            rcp->scn_ = scn;
            rcp->op_ = op;
            rcp->object_id_ = object_id;
            rcp->uflag_ = uflag;
            rcp->iflag_ = iflag;
            transit->second->changes_.insert(std::move(rcp));
          }
        }
      } break;
      case opcode::kMultiInsert:
      case opcode::kInsert: {
        for (auto row : redos) {
          RowChangePtr rcp(new RowChange());
          for (auto col : row) {
            // if (col->len_ > 0) {
            rcp->new_data_.push_back(col);
            rcp->uflag_ = uflag;
            rcp->iflag_ = iflag;
            //  }
          }
          rcp->scn_ = scn;
          rcp->op_ = op;
          rcp->object_id_ = object_id;
          transit->second->changes_.insert(std::move(rcp));
        }
      } break;

      case opcode::kUpdate: {
        // this is no mulitUpdate, so there is 1 elems in undo and redo at
        // most
        Row pk;
        auto undo_iter = undos.begin();
        auto redo_iter = redos.begin();
        RowChangePtr rcp(new RowChange());
        int ret = findPk(table_def, *undo_iter, pk);
        if (ret > 0) {
          rcp->pk_ = std::move(pk);
          rcp->scn_ = scn;
          rcp->op_ = op;
          rcp->object_id_ = object_id;
          rcp->uflag_ = uflag;
          rcp->iflag_ = iflag;
          rcp->new_data_ = std::move(*redo_iter);
          transit->second->changes_.insert(std::move(rcp));
        }
      } break;
      default:
        LOG(ERROR) << "Unknown Op " << (int)op << std::endl;
        break;
    }
  }

  bool verifyTrans(TransactionPtr trans_ptr) {
    bool dup = false;
#ifdef __STREAM_DEBUG__
    // we use scn to order changes, so we don't want 2 exactly same
    // scn in a given transaction
    std::map<SCN, uint32_t> scn_count;
    for (auto i : trans_ptr->changes_) {
      scn_count[i->scn_]++;
    }
    for (auto i : scn_count) {
      if (i.second > 1) {
        dup = true;
        break;
      }
    }

    if (dup) {
      LOG(WARNING) << "FOUND duplicated SCN in this transaction" << std::endl;
      LOG(WARNING) << trans_ptr->toString() << std::endl;
    }
#endif
    return ~dup;
  }
}
