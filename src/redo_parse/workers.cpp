#include <stdint.h>
#include <string.h>
#include <vector>
#include <sstream>
#include <thread>
#include <list>

#include "workers.h"
#include "redofile.h"
#include "util/container.h"
#include "util/dassert.h"
#include "physical_elems.h"
#include "logical_elems.h"
#include "opcode_ops.h"

namespace databus {
  using util::dassert;

  static void tranDump(XID xid, uint32_t object_id, const char* optype,
                       std::list<Row> undos, std::list<Row> redos) {
    return;  // temp disable the metadata dependency
             /*
           if (optype == NULL) return;
           TabDef* table_def = metadata->getTabDefFromId(object_id);
           std::cout << std::endl << std::endl << "Transaction ID " << xid
                     << std::endl;
           std::cout << optype << " " << table_def->name << std::endl;
           if (strncmp(optype, "insert", strlen("insert")) != 0) {
             std::cout << "Primary Keys:";
             for (auto undo : undos) {
               for (auto col : undo) {
                 if (col->len_ > 0 &&
                     table_def->pk.find(col->col_id_ + 1) != table_def->pk.end()) {
                   std::cout << "\t" << table_def->col_names[col->col_id_ + 1]
                             << "----" << col->content_;
                 }
               }
             }
             std::cout << std::endl;
           }
         
           if (strncmp(optype, "delete", strlen("delete")) != 0) {
             std::cout << "New data: " << std::endl;
             for (auto redo : redos) {
               for (auto col : redo) {
                 std::cout << table_def->col_names[col->col_id_ + 1] << "----"
                           << col->content_ << std::endl;
               }
             }
           }
           */
  }

  Reader::Reader(SList* record_pos_buf)
      : cur_redo_file_(NULL),
        record_pos_buf_(record_pos_buf),
        curr_done(true) {}

  void Reader::feed(const char* filename) {
    file_name_queue_.push_back(filename);
  }

  void Reader::parseRedo(const RedoFile* redo_file) {
    const char* record = redo_file->firstRecord();
    do {
      record_pos_buf_->push_back(record);
    } while ((record = redo_file->nextRecord(record)) != NULL);

    record_pos_buf_->push_back(NULL);
  }

  void Reader::resetCurLogfile(const char* filename) {
    RedoFile* redo_file = new RedoFile(filename);
    if (cur_redo_file_ != NULL) delete cur_redo_file_;
    cur_redo_file_ = redo_file;
    curr_done = false;
  }

  void Reader::run() {
    while (true) {
      const char* file_name = file_name_queue_.pop_front();
      if (curr_done) {
        resetCurLogfile(file_name);
        parseRedo(cur_redo_file_);
      } else {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }
  }

  void Reader::srun() {
    const char* file_name = file_name_queue_.pop_front();
    resetCurLogfile(file_name);
    parseRedo(cur_redo_file_);
  }

  void RecordBuilder::realCopyNBytes(const char* from, char* to, int32_t len) {
    int this_block_space_left = spaceLeft(from);
    if (this_block_space_left >= len) {
      memcpy(to, from, len);
      return;
    }
    uint32_t block_data_size =
        p_reader_->currentRedoLog()->blockSize() - constants::kBlockHeaderSize;

    memcpy(to, from, this_block_space_left);
    len -= this_block_space_left;
    to += this_block_space_left;
    from = from + this_block_space_left + constants::kBlockHeaderSize;

    while (len > block_data_size) {
      memcpy(to, from, block_data_size);
      len -= block_data_size;
      to += block_data_size;
      from = from + block_data_size + constants::kBlockHeaderSize;
    }

    memcpy(to, from, len);
  }

  void RecordBuilder::run() {
    while (true) {
      const char* record_pos = NULL;
      while ((record_pos = fetchARecordPos()) != NULL) {
        RecordBuf* record_buf = makeRecord(record_pos);
        if (record_buf != NULL) p_record_buf_list_->push_back(record_buf);
      }
      p_reader_->curr_done = true;
    }
  }

  void RecordBuilder::srun() {
    const char* record_pos = NULL;
    while ((record_pos = fetchARecordPos()) != NULL) {
      RecordBuf* record_buf = makeRecord(record_pos);
      if (record_buf != NULL) p_record_buf_list_->push_back(record_buf);
    }
    // only for test, add a NULL at last
    p_record_buf_list_->push_back(NULL);
  }

  RecordBuf* RecordBuilder::makeRecord(const char* record_start_pos) {
    Uchar ora_version = oraVersion();
    uint32_t record_len = immature::recordLength(record_start_pos, ora_version);

    int change_length = 0;
    const char* change_pos = NULL;
    SCN record_scn;
    uint32_t epoch = 0;

    if (ora_version == 9) {
      change_length = record_len - constants::kMinRecordLen;
      change_pos = record_start_pos + constants::kMinRecordLen;
      record_scn = As<RecordHeaderV9>(record_start_pos)->scn();
    } else {
      Uchar vld = immature::recordVld(record_start_pos, ora_version);

      if (immature::isMajor(vld)) {
        change_length = record_len - constants::kMinMajRecordLen;
        change_pos = record_start_pos + constants::kMinMajRecordLen;
        record_scn = As<RecordHeaderMajor>(record_start_pos)->scn();
        epoch = As<RecordHeaderMajor>(record_start_pos)->getEpoch();
      } else if (immature::isMinor(vld)) {
        change_length = record_len - constants::kMinRecordLen;
        change_pos = record_start_pos + constants::kMinRecordLen;
        record_scn = As<RecordHeaderMinor>(record_start_pos)->scn();
      } else {
        int ivld = vld;
        std::stringstream ss;
        ss << "unsupport vld " << ivld;
        dassert(ss.str().c_str(), false);
      }
    }

    if (change_length == 0) return NULL;
    if (p_reader_->cur_redo_file_->spaceLeft(change_pos) ==
        p_reader_->cur_redo_file_->blockSize())
      change_pos += constants::kBlockHeaderSize;

    size_t offset =
        record_start_pos - p_reader_->cur_redo_file_->fileStartPos();

    char* change_buf = new char[change_length];
    realCopyNBytes(change_pos, change_buf, change_length);

    RecordBuf* record_buf =
        new RecordBuf(record_scn, change_length, epoch, change_buf, offset);

    bool valid = true;
    uint32_t obj_id;
    for (auto c : record_buf->change_vectors) {
      if (c->opCode() == 0x0501) {
        obj_id = Ops0501::getObjId(c);
        if (!filter(obj_id)) valid = false;
      }
    }
    if (valid)
      return record_buf;
    else
      return NULL;
  }

  TransactionBuilder::TransactionBuilder(RecordList* p_record_buf_list,
                                         TransList* p_transaction_list)
      : record_buf_list_(p_record_buf_list),
        transaction_list_(p_transaction_list),
        handler_([](XID xid, uint32_t objid, const char* op,
                    std::list<Row>& undo, std::list<Row>& redo) { return; }) {}

  void dumpRow(Row row) {
    if (!row.empty()) std::cout << "New Values:" << std::endl;
    for (auto i : row) i->dump();
  }

  void TransactionBuilder::srun() {

    RecordBuf* record_buf;
    while ((record_buf = record_buf_list_->pop_front()) != NULL) {
      XID xid = 0;
      std::list<Row> undo, redo;
      uint32_t object_id;
      uint32_t data_object_id;
      const char* optype = NULL;
      for (auto i : record_buf->change_vectors) {
        switch (i->opCode()) {
          case opcode::kUndo: {
            xid = Ops0501::getXID(i);
            object_id = Ops0501::getObjId(i);
            data_object_id = Ops0501::getDataObjId(i);
            undo = Ops0501::makeUpUndo(i);
          } break;
          case opcode::kUpdate:
            redo = OpsDML::makeUpRedoCols(i);
            optype = "update";
            break;
          case opcode::kInsert:
          case opcode::kMultiInsert:
            redo = OpsDML::makeUpRedoCols(i);
            optype = "insert";
            break;
          case opcode::kDelete:
            optype = "delete";
            break;
        }  // end switch
      }
      if (optype != NULL) handler_(xid, object_id, optype, undo, redo);
    }
  }
}  // databus
