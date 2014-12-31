#ifndef REDOFILE_INC
#define REDOFILE_INC

#include <string>
#include <map>
#include <memory>
#include <stdint.h>
#include <functional>
#include "logical_elems.h"
#include "physical_elems.h"

namespace databus {

  typedef unsigned char Uchar;

  class RedoFile {
   public:
    // One RedoFile is a represent of a logfile whose
    // seq equals log_seq. Since the the same log_seq
    // maybe different logfile, so we can't initilize
    // RedoFile with a  char* filename any more
    RedoFile(uint32_t log_seq,
             std::function<std::string(uint32_t)> log_generator,
             std::function<uint32_t(uint32_t)> last_blk)
        : log_generator_(log_generator),
          log_sequence_(log_seq),
          online_last_blk_(last_blk),
          latest_blk_(0),
          file_start_pos_(NULL),
          length_(0),
          allop_(false) {
      std::string filename = log_generator_(log_seq);
      init(filename.c_str());
    };

    // this is just for some test purposes, works for archivelog only
    RedoFile(const char *filename)
        : latest_blk_(0xFFFFFFFF),
          file_start_pos_(NULL),
          length_(0),
          log_sequence_(-1),
          allop_(true) {
      log_generator_ = [&filename](uint32_t) { return filename; };
      online_last_blk_ = [](uint32_t) { return 0xFFFFFF; };
      init(filename);
    }

    // should return NULL if current logfile reach to
    // end.
    // even if during we read a online logfile,
    // the online logfile was re-write(switch logfile)
    // just turn to the archive log. Be sure to return
    // a valid RecordBuf with this call!
    std::shared_ptr<RecordBuf> nextRecordBuf();

    ~RedoFile();

   private:
    void init(const char *filename);
    // nextRecord will only return valided records, the validation rules
    // are defined here
    bool isValid(char *p_record);
    Ushort recordOffset(char *p);

    char *resetPosition(char *pos) {
      size_t offset = pos - file_start_pos_;
      init(log_generator_(log_sequence_).c_str());
      return file_start_pos_ + offset;
    }

    char *nextValid(char *, char *);
    char *firstRecord();
    // NULL if the end of file
    // switch to archive log file if overwrited
    // if no more data to read, just sleep and try again
    char *nextRecord(char *cur_record);

    void realCopyNBytes(char *from, char *to, uint32_t len);

    // A Record may take more than 1 blocks,
    // every block has a block header which can't store any record content
    // so if Record len is n, we need to advance more than n Bytes probably
    char *realAdvanceNBytes(char *p_record, size_t byte_count);

    // p_record here must be the exact begining of a block
    char *_realAdvanceNBytes(char *p_record, size_t byte_count);

    // given a position, get how many bytes left in that block
    uint32_t spaceLeft(char *p) {
      return block_size_ - (p - file_start_pos_) % block_size_;
    }

    Uchar oraVersion() { return ora_version_; }

    // start from 0
    uint32_t getBlockNo(char *p) {
      return (p - file_start_pos_) / block_size_;
    };

    bool isOverRead(uint32_t blk) {
      if (blk <= latest_blk_)
        return false;
      else
        latest_blk_ = online_last_blk_(log_sequence_);
      return latest_blk_ < blk;
    }

    bool isOverWrite() {
      switch (ora_version_) {
        case 9:
          return log_sequence_ !=
                 ((BlockHeaderV9 *)p_block_header_)->getSequenceNum();
        case 10:
        case 11:
          return log_sequence_ !=
                 ((BlockHeaderV10 *)p_block_header_)->getSequenceNum();
      }
      // never come here, see "notTestVersionDie(ora_version_)"
      return false;
    }

   private:
    uint32_t log_sequence_;

    std::function<std::string(uint32_t)> log_generator_;
    std::function<uint32_t(uint32_t)> online_last_blk_;
    uint32_t latest_blk_;

    char *file_start_pos_;
    char *p_block_header_;
    RedoHeader *p_redo_header_;
    SCN lowscn_;
    size_t length_;
    uint32_t block_size_;
    uint32_t last_block_id_;
    Uchar ora_version_;
    bool allop_;

    char *curr_record_pos_;
  };
}
#endif /* ----- #ifndef REDOFILE_INC  ----- */
