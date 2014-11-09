#include "logical_elems.h"
#include "physical_elems.h"
#include "opcode_ops.h"
#include "util/dtypes.h"
#include <iostream>

namespace databus {
  bool SCN::operator<(const SCN& other) const {
    uint64_t me = toNum();
    uint64_t you = other.toNum();
    if (me == you) {
      if (subscn_ == other.subscn_)
        return noffset_ < other.noffset_;
      else
        return subscn_ < other.subscn_;
    }
    return me < you;
  }

  uint64_t SCN::toNum() const {
    uint64_t n = 0;
    uint64_t major = major_;
    n = n | (major << (sizeof(minor_) * 8));
    n = n | minor_;
    return n;
  }

  std::string SCN::toStr() const {
    std::stringstream ss;
    ss << major_ << ":" << minor_ << ":" << subscn_ << ":" << noffset_;
    return ss.str();
  }
  RecordBuf::RecordBuf(const SCN& scn, uint32_t len, uint32_t epoch,
                       char* change_buf, size_t offset)
      : scn_(scn),
        change_length_(len),
        epoch_(epoch),
        change_buffers_(change_buf),
        offset_(offset) {
    initChangeVectors();
  }

  void RecordBuf::initChangeVectors() {
    // TODO:  bypass all the changes whose SEQ=0
    ChangeHeader* ch = (ChangeHeader*)(change_buffers_);
    uint32_t parsed_change_size = 0;
    do {
      uint32_t change_size = ch->changeSize();
      if (change_size == 0) {
        std::cout << "lol is 0, Diag offset ! " << offset_ << std::endl;
        return;
      }
      if (validOp(ch->opCode())) change_vectors.push_back(ch);
      parsed_change_size += change_size;
      unsigned int seq = ch->seq_;
      // std::cout << seq << ":" << std::hex << ch->opCode() << std::endl;
      ch = (ChangeHeader*)((char*)ch + change_size);
    } while (parsed_change_size != change_length_);
  }

  void ColumnChange::dump() {
    std::cout << "col# " << col_id_ << "  " << content_ << "(" << len_ << ")"
              << std::endl;
  }

  RowChange::~RowChange() {
    for (auto i : primary_key) delete i;
    for (auto i : undo_changes) delete i;
    for (auto i : redo_changes) delete i;
    for (auto i : redo_migration_changes) delete i;
  }
}
