#ifndef LOGICAL_ELEMS_H
#define LOGICAL_ELEMS_H

#include <string>
#include <sstream>
#include <stdint.h>
#include <map>
#include <memory>
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
              size_t offset, bool allop = false);

    std::list<ChangeHeader*> change_vectors;
    size_t offset() const { return offset_; }

    ~RecordBuf() { delete[] change_buffers_; }

   private:
    // allop:  if true, capatural all opocde. or else only captual valid op
    void initChangeVectors(bool allop);

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

  typedef std::list<std::shared_ptr<ColumnChange> > Row;
  class RowChange {
   public:
    ~RowChange();
    bool operator<(const RowChange& other) const { return scn_ < other.scn_; };
    SCN scn_;
    std::string optype_;
    uint32_t object_id_;
    uint32_t data_object_id_;
    Row primary_key_;
    Row undo_changes_;
    Row redo_changes_;
    // Row redo_migration_changes_;
  };
}  // databus
#endif
