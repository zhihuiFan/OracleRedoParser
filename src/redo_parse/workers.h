#ifndef _DATABUS_REDOPARSE_WORKERS_H
#define _DATABUS_REDOPARSE_WORKERS_H 1
#include <list>
#include <stdint.h>
#include <functional>
#include "util/container.h"
#include "util/dtypes.h"
#include "redofile.h"

namespace databus {
  class RecordBuf;
  class Transaction;

  typedef List<const char*> SList;
  typedef List<RecordBuf*> RecordList;
  typedef List<Transaction*> TransList;

  class Reader {
    friend class RecordBuilder;

   public:
    Reader(SList* record_pos_buf);
    const RedoFile* currentRedoLog() const { return cur_redo_file_; }
    void feed(const char* filename);
    void run();

    // only for test
    void srun();

   private:
    void resetCurLogfile(const char* filename);
    void parseRedo(const RedoFile* redo_file);

    SList file_name_queue_;
    SList* record_pos_buf_;
    RedoFile* cur_redo_file_;
    // set it to true when
    // 1. the cur_redo_files parse completed
    // AND
    // 2. builder have build all the record from it
    bool curr_done;
  };

  // multi-threads:
  // we have have mulit threads to build_records from `records_start_pos` at the
  // same time, most of time costing should be copy data from memory
  // TODO: when to release resould for records_buf_list

  class RecordBuilder {
   public:
    RecordBuilder(Reader* reader, RecordList* record_buf_list)
        : p_reader_(reader), p_record_buf_list_(record_buf_list) {
      filter = [](uint32_t o) { return true; };
    };

    template <class Filter>
    void setFilter(Filter fn) {
      filter = fn;
    }

    void run();
    // only for test
    void srun();

   private:
    uint32_t spaceLeft(const char* p) const {
      return p_reader_->currentRedoLog()->spaceLeft(p);
    }

    Uchar oraVersion() const {
      return p_reader_->currentRedoLog()->oraVersion();
    }

    const char* fetchARecordPos() {
      return p_reader_->record_pos_buf_->pop_front();
    }

    void realCopyNBytes(const char* from, char* to, int32_t len);
    RecordBuf* makeRecord(const char* record_start_pos);

   private:
    Reader* p_reader_;
    RecordList* p_record_buf_list_;
    std::function<bool(uint32_t)> filter;
  };

  class TransactionBuilder {
   public:
    TransactionBuilder(RecordList* record_buf_list,
                       TransList* transaction_list);
    // only for test
    void srun();
    template <class Handler>
    void setHandler(Handler handler) {
      handler_ = handler;
    }

   private:
    void parseRecord(const RecordBuf* buf);
    // init Transaction instance if needed,  the result
    RowChange* buildRowChange(const ChangeHeader* change_header);

   private:
    std::function<void(XID, uint32_t, const char*, std::list<Row>&,
                       std::list<Row>&)> handler_;

    RecordList* record_buf_list_;
    TransList* transaction_list_;

    std::map<uint64_t, XID> fba2xid_;
  };

  // Single Threads:
  // TODO: make is multi-threadable
  // void BuildTransaction(RecordList* records_buf_list, TransList* trans_list);

  // Single Thread:
  // It would be interesting to make apply thread is mulit-threadable,
  // single thread now
  // void ApplyTransaction(TransList* trans_list, MetadataManager&);
}

#endif
