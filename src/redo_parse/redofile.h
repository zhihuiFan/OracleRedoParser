#ifndef REDOFILE_INC
#define REDOFILE_INC

#include "logical_elems.h"
#include <string>
#include <map>
#include <stdint.h>

namespace databus {

  typedef unsigned char Uchar;

  class RedoFile {
   public:
    explicit RedoFile(const char *filename);
    const char *firstRecord() const;
    const char *nextRecord(const char *cur_record) const;
    Uchar oraVersion() const { return ora_version_; }

    // given a position, get how many bytes left in that block
    uint32_t spaceLeft(const char *p) const {
      return block_size_ - (p - file_start_pos_) % block_size_;
    }

    uint32_t blockSize() const { return block_size_; }

    const char *fileStartPos() { return file_start_pos_; }

    ~RedoFile();

   private:
    // nextRecord will only return valided records, the validation rules
    // are defined here
    bool isValid(const char *p_record) const;
    Ushort recordOffset(const char *p) const;

    // A Record may take more than 1 blocks,
    // every block has a block header which can't store any record content
    // so if Record len is n, we need to advance more than n Bytes probably
    const char *realAdvanceNBytes(const char *p_record,
                                  size_t byte_count) const;

    // p_record here must be the exact begining of a block
    const char *_realAdvanceNBytes(const char *p_record,
                                   size_t byte_count) const;

    // start from 0
    uint32_t getBlockNo(const char *p) const {
      return (p - file_start_pos_) / block_size_;
    };

   private:
    char *file_start_pos_;
    size_t length_;

    uint32_t block_size_;
    uint32_t last_block_id_;

    std::string db_name_;
    uint32_t log_sequence_;

    SCN lowscn_;
    SCN nextscn_;

    Uchar ora_version_;
  };
}
#endif /* ----- #ifndef REDOFILE_INC  ----- */
