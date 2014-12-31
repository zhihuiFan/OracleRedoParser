#include "trans.h"
#include "opcode.h"
#include "opcode_ops.h"
#include "metadata.h"
#include "tconvert.h"
#include "util/logger.h"

namespace databus {
  DBAMap Transaction::getDBAMap() {
    static std::map<DBA, USN> xid2uba;
    return xid2uba;
  }
  XIDMap Transaction::getXIDMap() {
    static std::map<XID, TransactionPtr> xid_map;
    return xid_map;
  }

  std::string Transaction::toString() const {
    std::stringstream ss;
    ss << std::endl << "start_scn : " << start_scn_.toStr() << std::endl
       << "commit_scn: " << commit_scn_.toStr() << std::endl
       << "commited  : " << (int)commited_ << std::endl;
    for (auto rc : changes_) {
      ss << rc->toString();
    }
    return ss.str();
  }

  static std::string colAsStr(ColumnChangePtr col, TabDefPtr tab_def,
                              char seperator = ':') {
    std::stringstream ss;
    ss << tab_def->col_names[col->col_id_ + 1] << seperator
       << convert(col->content_, tab_def->col_types[col->col_id_ + 1],
                  col->len_) << ",";
    return ss.str();
  }

  RowChange::RowChange()
      : scn_(), op_(Op::NA), object_id_(0), pk_{}, new_data_{} {}
  std::string RowChange::toString() const {
    std::stringstream ss;
    TabDefPtr tab_def = metadata->getTabDefFromId(object_id_);
    ss << std::endl << "Change SCN " << scn_.toStr() << std::endl
       << opmap.at(op_) << tab_def->owner << "." << tab_def->name << std::endl;
    switch (op_) {
      case Op::INSERT:
      case Op::MINSERT:
        for (auto col : new_data_) {
          ss << colAsStr(col, tab_def, '=') << std::endl;
        }
        ss << "\n";
        break;
      case Op::UPDATE:
        ss << " SET ";
        for (auto col : new_data_) {
          ss << colAsStr(col, tab_def, '=') << std::endl;
        }
        ss << "WHERE ";
        for (auto col : pk_) {
          ss << colAsStr(col, tab_def, '=') << std::endl << " AND ";
        }
        ss << " 1=1; ";
        break;
      case Op::DELETE:
        ss << "WHERE ";
        for (auto col : pk_) {
          ss << colAsStr(col, tab_def, '=') << std::endl << " AND ";
        }
        ss << " 1=1; ";
        break;
      default:
        BOOST_LOG_TRIVIAL(fatal) << "Unknown Op " << (int)op_;
    }
    return ss.str();
  }

  void buildTransaction(RecordBufPtr record) {
    DBA dba = 0;
    XID xid = 0;
    std::list<Row> undo, redo;
    uint32_t object_id;
    SCN record_scn = ((RecordHeaderMinor*)(record.get()))->scn();
    SCN trans_start_scn;
    SCN zeroSCN;
    Op op = Op::NA;
    for (auto change : record->change_vectors) {
      switch (change->opCode()) {
        case opcode::kBeginTrans:
          dba = change->dba();
          {
            if (((OpCode0502*)change->part(1))->sqn_ > 0) {
              trans_start_scn = record_scn;
            }
          }
          break;
        case opcode::kUndo:
          xid = Ops0501::getXID(change);
          object_id = Ops0501::getObjId(change);
          if (dba > 0) {
            Transaction::getDBAMap()[dba] =
                ((OpCode0501*)change->part(1))->xid_high_;
            dba = 0;
            if (zeroSCN < trans_start_scn) {
              XIDMap xidmap = Transaction::getXIDMap();
              auto it = xidmap.find(xid);
              if (it != xidmap.end()) {
                BOOST_LOG_TRIVIAL(fatal)
                    << "I think Transaction (xid" << xid
                    << ") just start here offset: " << record->offset()
                    << " but it was started before \n"
                    << "Dump Info\n" << it->second->toString();
                return;
              }
              xidmap[xid] = TransactionPtr(new Transaction());
              xidmap[xid]->start_scn_ = record_scn;
            }
          }
          undo = Ops0501::makeUpUndo(change);
          break;
        case opcode::kUpdate:
          redo = OpsDML::makeUpRedoCols(change);
          op = Op::UPDATE;
          break;
        case opcode::kInsert:
        case opcode::kMultiInsert:
          redo = OpsDML::makeUpRedoCols(change);
          op = Op::INSERT;
          break;
        case opcode::kDelete:
          op = Op::DELETE;
          break;
        case opcode::kCommit:
          dba = change->dba();
          {
            auto it = Transaction::getDBAMap().find(dba);
            if (it == Transaction::getDBAMap().end()) {
              BOOST_LOG_TRIVIAL(warning)
                  << "found dba " << dba
                  << " in commit , but unknow when this transaction is started";
              return;
            }
            OpCode0504_ucm* ucm = (OpCode0504_ucm*)(change->part(1));
            XID ixid =
                (((XID)it->second) << (sizeof(Ushort) + sizeof(uint32_t)) * 8) |
                (((XID)ucm->slt_) << sizeof(uint32_t) * 8) | ucm->sqn_;
            auto xidit = Transaction::getXIDMap().find(ixid);
            if (xidit == Transaction::getXIDMap().end()) {
              BOOST_LOG_TRIVIAL(warning)
                  << "found xid " << dba
                  << " in commit , but unknow when this transaction is started";
              return;
            }
            xidit->second->commit_scn_ = record_scn;
            xidit->second->commited_ = ucm->flg_;
            /*
            switch (ucm->flg_) {
              case 4:
                xidit->commited_ = 2;
                break;
              case 2:
                xidit->commited_ = 1;
                break;
              default:
                BOOST_LOG_TRIVIAL(warning)
                    << "Found known commit flg at offset: " << record->offset()
                    << std:: : endl;
                return;
            }
            */
          }

      }  // end switch
    }
    if (op != Op::NA)
      makeTranRecord(xid, object_id, op, undo, redo, record_scn);
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
      BOOST_LOG_TRIVIAL(fatal) << "Number of PK mismatched\n Object Name "
                               << table_def->owner << "." << table_def->name
                               << "\nNum of PK " << table_def->pk.size()
                               << "\nNo. of PK in Redo " << pk.size();
      return -1;
    }
  }
  void makeTranRecord(XID xid, uint32_t object_id, Op op, std::list<Row>& undos,
                      std::list<Row>& redos, const SCN& scn) {
    XIDMap xidmap = Transaction::getXIDMap();
    auto transit = xidmap.find(xid);
    if (transit == xidmap.end()) {
      BOOST_LOG_TRIVIAL(fatal)
          << "XID " << xid
          << " info was missed when I want to add a change to it";
      return;
    }
    auto table_def = metadata->getTabDefFromId(object_id);
    if (table_def == NULL) {
      BOOST_LOG_TRIVIAL(warning) << "Can't get table definition for object_id "
                                 << object_id << " ignore this change ";
      return;
    }
    switch (op) {
      case Op::DELETE: {
        for (auto row : undos) {
          RowChangePtr rcp(new RowChange());
          Row pk;
          int ret = findPk(table_def, row, pk);
          if (ret > 0) {
            rcp->pk_ = std::move(pk);
            rcp->scn_ = scn;
            rcp->op_ = op;
            rcp->object_id_ = object_id;
            transit->second->changes_.insert(std::move(rcp));
          }
        }
      } break;
      case Op::INSERT: {
        for (auto row : redos) {
          RowChangePtr rcp(new RowChange());
          rcp->new_data_ = std::move(row);
          rcp->scn_ = scn;
          rcp->op_ = op;
          rcp->object_id_ = object_id;
          transit->second->changes_.insert(std::move(rcp));
        }
      } break;
      case Op::UPDATE: {
        // this is no mulitUpdate, so there is 1 elems in undo and redo at most
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
          rcp->new_data_ = std::move(*redo_iter);
          transit->second->changes_.insert(std::move(rcp));
        }
      } break;
      default:
        BOOST_LOG_TRIVIAL(fatal) << "Unknown Op " << (int)op;
        break;
    }
  }
}
