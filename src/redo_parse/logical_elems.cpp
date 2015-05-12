#include <iostream>
#include <sstream>
#include <iomanip>

#include "logical_elems.h"
#include "physical_elems.h"
#include "opcode_ops.h"
#include "util/dtypes.h"
#include "opcode.h"

namespace databus {

  const std::set<Ushort> kDMLOps{opcode::kInsert,      opcode::kDelete,
                                 opcode::kUpdate,      opcode::kRowChain,
                                 opcode::kBeginTrans,  opcode::kCommit,
                                 opcode::kMultiInsert, opcode::kMultiDelete,
                                 opcode::kLmn,  // 11.16
                                 opcode::kBeginTrans,  opcode::kCommit};

  const std::set<Ushort> kTRANOps{opcode::kUndo};

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
    ss << std::hex << major_ << ":" << minor_ << ":" << subscn_ << ":"
       << noffset_;
    return ss.str();
  }

  std::string SCN::toString() const {
    std::stringstream ss;
    ss << std::hex << std::setfill('0') << std::setw(4) << major_
       << std::setw(8) << minor_ << std::setw(8) << subscn_ << std::setw(8)
       << noffset_;
    return ss.str();
  }
  RecordBuf::RecordBuf(const SCN& scn, uint32_t len, uint32_t epoch,
                       char* change_buf, size_t offset, bool allop)
      : scn_(scn),
        change_length_(len),
        epoch_(epoch),
        change_buffers_(change_buf),
        op_(0),
        offset_(offset) {
    initChangeVectors(allop);
  }

  void RecordBuf::initChangeVectors(bool allop) {
    // TODO:  bypass all the changes whose SEQ=0
    ChangeHeader* ch = (ChangeHeader*)(change_buffers_);
    uint32_t parsed_change_size = 0;
    bool keep = false;
    do {
      uint32_t change_size = ch->changeSize();
      if (change_size == 0) {
        LOG(DEBUG) << "lol is 0, Diag offset ! " << offset_;
        return;
      }
      if (kDMLOps.find(ch->opCode()) != kDMLOps.end()) {
        change_vectors.push_back(ch);
        keep = true;
        op_ = ch->opCode();
      } else if (kTRANOps.find(ch->opCode()) != kTRANOps.end()) {
        change_vectors.push_back(ch);
      } else if (allop)
        change_vectors.push_back(ch);
      parsed_change_size += change_size;
      // unsigned int seq = ch->seq_;
      // std::cout << seq << ":" << std::hex << ch->opCode() << std::endl;
      ch = (ChangeHeader*)((char*)ch + change_size);
    } while (parsed_change_size != change_length_);
    if (!keep) change_vectors.clear();
  }

  void ColumnChange::dump() {
    std::cout << "col# " << col_id_ << "  " << content_ << "(" << len_ << ")"
              << std::endl;
  }
}
