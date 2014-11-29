#ifndef LOGICAL_ELEMS_H
#define LOGICAL_ELEMS_H

#include <string>
#include <sstream>
#include <stdint.h>
#include <map>
#include <list>
#include "util/dtypes.h"

namespace databus {

  class ChangeHeader;
  class SCN {
   public:
    SCN() : major_(0), minor_(0), subscn_(0), noffset_(0) {}
    SCN(Ushort maj, uint32_t minor, unsigned int subscn = 0, int noffset = 0)
        : major_(maj), minor_(minor), subscn_(subscn), noffset_(noffset) {}

    bool operator<(const SCN& other) const;
    std::string toStr() const;

    Ushort major_;
    uint32_t minor_;
    unsigned int subscn_;
    int noffset_;

   private:
    uint64_t toNum() const;
  };  // SCN

  class RecordBuf {
   public:
    RecordBuf(const SCN& scn, uint32_t len, uint32_t epoch, char* change_buf,
              size_t offset);

    std::list<ChangeHeader*> change_vectors;
    size_t offset() const { return offset_; }

    ~RecordBuf() { delete[] change_buffers_; }

   private:
    void initChangeVectors();

   private:
    SCN scn_;
    uint32_t epoch_;
    uint32_t change_length_;
    char* change_buffers_;
    size_t offset_;  // debug only
  };                 // RecordBuf

  class ColumnChange {
   public:
    ColumnChange(Ushort col_id, Ushort len, char* content)
        : col_id_(col_id), len_(len), content_(content) {}
    ~ColumnChange() { delete[] content_; }
    void dump();

    Ushort col_id_;
    Ushort len_;
    char* content_;
  };

  typedef std::list<ColumnChange*> Row;
  class RowChange {
   public:
    ~RowChange();

    std::string optype;
    uint32_t object_id_;
    uint32_t data_object_id_;
    Row primary_key;
    Row undo_changes;
    Row redo_changes;
    Row redo_migration_changes;
  };

  class Transaction {
   public:
    ~Transaction() {
      for (auto i : trans_changes_) delete i;
    }

   private:
    XID xid_;
    std::list<RowChange*> trans_changes_;
  };
}  // databus
#endif
