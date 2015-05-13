#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <map>
#include <list>
#include <vector>
#include <sstream>
#include <thread>
#include "trans.h"
#include "opcode.h"
#include "opcode_ops.h"
#include "metadata.h"
#include "tconvert.h"
#include "util/logger.h"
#include "util/dassert.h"
#include "stream.h"
#include "applier.h"

namespace databus {
  bool Transaction::operator<(const Transaction& other) const {
    return commit_scn_ < other.commit_scn_;
  }

  DBAMap Transaction::dba_map_;
  XIDMap Transaction::xid_map_;
  std::map<SCN, TransactionPtr> Transaction::commit_trans_;

  SCN Transaction::last_commit_scn_;
  SCN Transaction::restart_scn_;
  std::mutex Transaction::restart_mutex_;
  std::mutex Transaction::commit_mutex_;

  SCN Transaction::getLastCommitScn() {
    std::lock_guard<std::mutex> lk(commit_mutex_);
    return last_commit_scn_;
  }
  SCN Transaction::getRestartScn() {
    std::lock_guard<std::mutex> lk(restart_mutex_);
    return restart_scn_;
  }
  void Transaction::setCommitScn(const SCN& scn) {
    std::lock_guard<std::mutex> lk(commit_mutex_);
    if (last_commit_scn_ < scn) {
      last_commit_scn_ = scn;
    }
  }

  void Transaction::setRestartScn(const SCN& scn) {
    std::lock_guard<std::mutex> lk(restart_mutex_);
    if (scn < restart_scn_) {
      restart_scn_ = scn;
    }
  }
  XIDMap::iterator buildTransaction(XIDMap::iterator it) {
    if (it->second->has_rollback()) {
      return Transaction::xid_map_.erase(it);
    } else if (it->second->has_commited()) {
      if (it->second->commit_scn_ < Transaction::getLastCommitScn()) {
        return Transaction::xid_map_.erase(it);
      }
      it->second->tidyChanges();
      Transaction::commit_trans_[it->second->commit_scn_] =
          Transaction::xid_map_[it->second->xid_];
      return Transaction::xid_map_.erase(it);
    }
    return Transaction::xid_map_.end();
  }

  bool Transaction::lastCompleted() const {
    if (changes_.empty()) return true;
    auto it = changes_.end();
    --it;
    return (*it)->completed();
  }

  void Transaction::merge(RowChangePtr r) {
    if (changes_.empty()) {
      changes_.insert(r);
      return;
    }
    if (lastCompleted() && r->op_ != opcode::kRowChain) {
      // 11.2 may be completed already, but we still need merge this rowchain to
      // it if its cc != 0
      changes_.insert(r);
      return;
    }
    if (r->iflag_ == 0x2c) {
      if (!lastCompleted()) {
        LOG(ERROR) << "Error";
        std::exit(20);
      } else {
        changes_.insert(r);
        return;
      }
    }

    auto it = changes_.end();
    --it;
    std::stringstream ss;
    if (r->object_id_ != (*it)->object_id_) {
      ss << "Object ID mismatched  Previous Object_id " << (*it)->object_id_
         << " Current Object_id " << r->object_id_;
    }

    // some pre-check before merge
    if ((*it)->op_ == opcode::kInsert) {
      if (r->op_ != opcode::kInsert && r->op_ != opcode::kRowChain) {
        ss << " opcode miss match, No " << (*it)->op_
           << " can be followed by a " << r->op_;
      }
    } else if ((*it)->op_ == opcode::kDelete) {
      if (r->op_ != opcode::kDelete && r->op_ != opcode::kRowChain) {
        ss << " opcode miss match, No " << (*it)->op_
           << " can be followed by a " << r->op_;
      }
    } else if ((*it)->op_ == opcode::kUpdate) {
      if (r->op_ != opcode::kUpdate && r->op_ != opcode::kRowChain &&
          r->op_ != opcode::kLmn) {
        ss << " opcode miss match, No " << (*it)->op_
           << " can be followed by a " << r->op_;
      }
    } else if ((*it)->op_ == opcode::kRowChain) {
      if (r->op_ != opcode::kDelete) {
        ss << " opcode miss match, No " << (*it)->op_
           << " can be followed by a " << r->op_;
      }
    }

    if (!ss.str().empty()) {
      ss << " Pevious SCN " << (*it)->scn_.toStr() << " Current SCN "
         << r->scn_.toStr();
      LOG(ERROR) << ss.str();
      return;
    }

    auto new_pk_cnt = (*it)->new_pk_.size();
    auto old_pk_cnt = (*it)->old_pk_.size();
    (*it)->new_pk_.insert(r->new_pk_.begin(), r->new_pk_.end());
    (*it)->old_pk_.insert(r->old_pk_.begin(), r->old_pk_.end());
    if (new_pk_cnt + r->new_pk_.size() != (*it)->new_pk_.size() ||
        old_pk_cnt + r->old_pk_.size() != (*it)->old_pk_.size()) {
      ss << "Found duplicated PK in the 2 changes";
    }

    if (!ss.str().empty()) {
      ss << " Pevious SCN " << (*it)->scn_.toStr() << " Current SCN "
         << r->scn_.toStr();
      LOG(ERROR) << ss.str();
      return;
    }

    if ((*it)->op_ == opcode::kInsert && r->op_ == opcode::kRowChain) {
      (*it)->op_ = opcode::kUpdate;
    }
  }

  void Transaction::tidyChanges() {
    auto temp_changes = std::move(changes_);
    for (auto& r : temp_changes) {
      switch (r->op_) {
        case opcode::kMultiInsert:
          changes_.insert(r);
          break;
        default:
          merge(r);
          break;
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

  std::string colAsStr2(ColumnChangePtr col, TabDefPtr tab_def) {
    std::stringstream ss;
    ss << convert(col->content_, tab_def->col_types[col->col_id_ + 1],
                  col->len_);
    return ss.str();
  }

  Ushort findPk(std::shared_ptr<TabDef> table_def, const Row& undo,
                OrderedPK& pk) {
    for (const auto col : undo) {
      if (col->col_id_ > 256) {
        LOG(WARNING) << "Found column ID " << col->col_id_
                     << " probably this is a program bug";
      }
      if (col->len_ > 0 &&
          table_def->pk.find(col->col_id_ + 1) != table_def->pk.end()) {
        pk.insert(col);
      }
    }
    return pk.size();
  }

  RowChange::RowChange()
      : scn_(),
        object_id_(0),
        op_(),
        uflag_(0),
        iflag_(0),
        start_col_(0),
        cc_(0),
        old_pk_{},
        new_pk_{} {}

  std::string RowChange::toString() const {
    std::stringstream ss;
    ss << " Object_id " << object_id_ << " Op " << op_ << " Start_col "
       << start_col_ << " Offset " << scn_.noffset_;
    for (auto c : old_pk_) {
      ss << " old_pk_c_" << c->col_id_;
    }

    for (auto c : new_pk_) {
      ss << " new_pk_c_" << c->col_id_;
    }
    return ss.str();
  }

  bool RowChange::completed() const {
    auto tab_def = getMetadata().getTabDefFromId(object_id_);
    switch (op_) {
      case opcode::kInsert:
      case opcode::kMultiInsert:
        return new_pk_.size() == tab_def->pk.size();
      case opcode::kUpdate:
      case opcode::kDelete:
      case opcode::kLmn:
      case opcode::kRowChain:
        return old_pk_.size() == tab_def->pk.size();
      default:
        return false;
    }
  }
  std::string RowChange::pkToString() const {
    std::stringstream ss;
    TabDefPtr tab_def = getMetadata().getTabDefFromId(object_id_);
    ss << tab_def->owner << "." << tab_def->name << "  " << getOpStr(op_);
    ss << " New PK ";
    // for update, delete, row migration, we store pk into pk_ already
    for (ColumnChangePtr c : new_pk_) {
      ss << colAsStr(c, tab_def);
    }
    ss << " Old PK ";
    for (ColumnChangePtr c : old_pk_) {
      ss << colAsStr(c, tab_def);
    }
    return ss.str();
  }

  std::vector<std::string> RowChange::getPk() {
    // the pk string is order by col_no
    TabDefPtr tab_def = getMetadata().getTabDefFromId(object_id_);
    // this line is super urgly and error prone, will fix it someday
    std::vector<std::string> pks(new_pk_.size() + old_pk_.size() +
                                 prefix_cols.size() - 1);
    int n = 0;
    pks[n++] = getOpStr(op_);
    pks[n++] = scn_.toString();
    for (ColumnChangePtr c : old_pk_) {
      pks[n++] = std::move(colAsStr2(c, tab_def));
    }
    for (ColumnChangePtr c : new_pk_) {
      pks[n++] = std::move(colAsStr2(c, tab_def));
    }
    return pks;
  }

  void addToTransaction(RecordBufPtr record) {
    DBA dba = 0;
    XID xid = 0;
    std::list<Row> undo, redo;
    SCN trans_start_scn;
    // OpCodeSupplemental* opsup = NULL;
    RowChangePtr rcp(new RowChange());
    rcp->scn_ = record->scn();
    for (auto change : record->change_vectors) {
      switch (change->opCode()) {
        case opcode::kBeginTrans:
          dba = change->dba();
          {
            if (((OpCode0502*)change->part(1))->sqn_ > 0) {
              // sqn_ == 0 is not a start of transaction
              trans_start_scn = rcp->scn_;
              Transaction::setRestartScn(trans_start_scn);
            }
          }
          break;
        case opcode::kUndo:
          xid = Ops0501::getXID(change);
          if (xid == 0) return;
          rcp->object_id_ = Ops0501::getObjId(change);
          if (dba > 0) {
            Transaction::dba_map_[dba] =
                ((OpCode0501*)change->part(1))->xid_high_;
            dba = 0;
            if (!trans_start_scn.empty()) {
              auto& xidmap = Transaction::xid_map_;
              auto it = xidmap.find(xid);
              if (it != xidmap.end()) {
                LOG(DEBUG) << "I think Transaction (xid" << xid
                           << ") just start here offset: " << record->offset()
                           << " but it was started before \n"
                           << "Dump Info\n" << it->second->toString()
                           << std::endl;
                return;
              }
              xidmap[xid] = TransactionPtr(new Transaction());
              xidmap[xid]->start_scn_ = rcp->scn_;
              xidmap[xid]->xid_ = xid;
            }
          }
          {
            if (getMetadata().getTabDefFromId(rcp->object_id_, false) == NULL) {
              // we don't care about this object id for version 0.1
              return;
            }
            undo = Ops0501::makeUpUndo(change, rcp);
            // opsup only set when  irp->xtype_ & 0x20
            // LOG(INFO) << "Sup start_col_offset " << record->offset() << ":"
            //      << opsup->start_column_ << ":" << opsup->start_column2_
            //    << std::endl;
          }
          break;
        case opcode::kUpdate:
        case opcode::kInsert:
        case opcode::kRowChain:
        case opcode::kMultiInsert:
          rcp->op_ = change->opCode();
          redo = OpsDML::makeUpRedoCols(change, rcp);
          break;
        case opcode::kDelete:
        case opcode::kLmn:
          rcp->op_ = change->opCode();
          break;
        case opcode::kCommit:
          dba = change->dba();
          {
            auto it = Transaction::dba_map_.find(dba);
            if (it == Transaction::dba_map_.end()) {
              LOG(DEBUG) << "found dba " << dba
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
              LOG(DEBUG) << "found xid " << dba
                         << " in commit , but unknow when "
                            "this transaction is started" << std::endl;
              return;
            }
            xidit->second->commit_scn_ = rcp->scn_;
            xidit->second->commited_ = ucm->flg_;
          }
      }  // end switch
    }
    if (rcp->op_ && xid != 0) makeTranRecord(xid, rcp, undo, redo);
  }

  void makeTranRecord(XID xid, RowChangePtr rcp, std::list<Row>& undos,
                      std::list<Row>& redos) {
    XIDMap& xidmap = Transaction::xid_map_;
    auto transit = xidmap.find(xid);
    if (transit == xidmap.end()) {
      LOG(DEBUG) << "XID " << xid
                 << " info was missed when I want to add a change to it"
                 << std::endl;
      return;
    }
    auto table_def = getMetadata().getTabDefFromId(rcp->object_id_);
    if (table_def == NULL) {
      LOG(DEBUG) << "Can't get table definition for object_id "
                 << rcp->object_id_ << " ignore this change " << std::endl;
      return;
    }
    LOG(DEBUG) << getOpStr(rcp->op_) << " " << rcp->scn_.noffset_
               << " start_col  " << rcp->start_col_ << " Offset "
               << rcp->scn_.noffset_ << std::endl;

    switch (rcp->op_) {
      case opcode::kDelete:
      case opcode::kLmn: {
        for (auto row : undos) {
          findPk(table_def, row, rcp->old_pk_);
          // even the old_pk_ is null, we still need it to mark a 11.6 completed
          transit->second->changes_.insert(std::move(rcp));
        }
      } break;
      case opcode::kMultiInsert: {
        for (auto row : redos) {
          findPk(table_def, row, rcp->new_pk_);
          if (!rcp->old_pk_.empty())
            transit->second->changes_.insert(std::move(rcp));
        }
      } break;

      case opcode::kUpdate:
      case opcode::kInsert:
      case opcode::kRowChain: {
        // this is no mulitUpdate, so there is 1 elems in undo and redo at
        // most
        OrderedPK pk;
        if (!undos.empty()) {
          auto undo_iter = undos.begin();
          findPk(table_def, *undo_iter, rcp->old_pk_);
        }
        if (!redos.empty()) {
          auto redo_iter = redos.begin();
          findPk(table_def, *redo_iter, rcp->new_pk_);
        }
        if (!rcp->old_pk_.empty() || !rcp->new_pk_.empty()) {
          transit->second->changes_.insert(std::move(rcp));
        }
      } break;
      default:
        LOG(ERROR) << "Unknown Op " << (int)rcp->op_ << std::endl;
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
