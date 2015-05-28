#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <algorithm>
#include <map>
#include <iostream>
#include <string>
#include <assert.h>

#include "redofile.h"
#include "physical_elems.h"
#include "util/dassert.h"
#include "opcode_ops.h"
#include "util/logger.h"

namespace databus {
  using util::dassert;

  void RedoFile::init(const char* filename) {
    LOG(INFO) << "RedoFile::init " << filename << std::endl;

    if (file_start_pos_ != NULL) munmap(file_start_pos_, length_);
    int fd = open(filename, O_RDONLY, 0644);

    dassert(strerror(errno), fd != -1, errno);

    struct stat stats;
    dassert(strerror(errno), fstat(fd, &stats) == 0, errno);
    off_t l = 4294967296;
    dassert("Redo file is larger than 4G, exiting..", stats.st_size <= l);
    // we need to change scn offset_ if this happened
    length_ = stats.st_size;
    file_start_pos_ = (char*)mmap(0, length_, PROT_READ, MAP_SHARED, fd, 0);
    dassert(strerror(errno), file_start_pos_ != NULL, errno);
    close(fd);

    ora_version_ = immature::oracleVersion(file_start_pos_);

    switch (ora_version_) {
      case 9:
        block_size_ = As<FileHeaderV9>(file_start_pos_)->getBlockSize();
        last_block_id_ = As<FileHeaderV9>(file_start_pos_)->getLastBlockID();
        p_block_header_ = file_start_pos_ + block_size_;
        if (log_sequence_ == -1) {
          log_sequence_ = ((BlockHeaderV9*)p_block_header_)->getSequenceNum();
        }
        assert(log_sequence_ ==
               ((BlockHeaderV9*)p_block_header_)->getSequenceNum());
        break;
      case 10:
      case 11:
        block_size_ = As<FileHeaderV10>(file_start_pos_)->getBlockSize();
        last_block_id_ = As<FileHeaderV10>(file_start_pos_)->getLastBlockID();
        p_block_header_ = file_start_pos_ + block_size_;
        if (log_sequence_ == -1) {
          log_sequence_ = ((BlockHeaderV10*)p_block_header_)->getSequenceNum();
        }
        dassert("", log_sequence_ ==
                        ((BlockHeaderV10*)p_block_header_)->getSequenceNum());
        break;
      default:
        immature::notTestedVersionDie(ora_version_);
    }

    p_redo_header_ = As<RedoHeader>(file_start_pos_ + block_size_ +
                                    constants::kBlockHeaderSize);
    lowscn_ = p_redo_header_->lowScn();
    curr_record_pos_ = firstRecord();
  }

  RedoFile::~RedoFile() {
    if (file_start_pos_ != NULL) munmap(file_start_pos_, length_);
  }

  void RedoFile::realCopyNBytes(char* from, char* to, uint32_t len) {
    uint32_t this_block_space_left = spaceLeft(from);
    if (this_block_space_left >= len) {
      memcpy(to, from, len);
      return;
    }

    uint32_t block_data_size = block_size_ - constants::kBlockHeaderSize;

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

  RecordBufPtr RedoFile::nextRecordBuf() {
  again:
    if (curr_record_pos_ == NULL) return NULL;  // eof
    uint32_t record_len =
        immature::recordLength(curr_record_pos_, ora_version_);
    uint32_t change_length = 0;
    char* change_pos = NULL;
    SCN record_scn;

    if (ora_version_ == 9) {
      change_length = record_len - constants::kMinRecordLen;
      change_pos = curr_record_pos_ + constants::kMinRecordLen;
      record_scn = As<RecordHeaderV9>(curr_record_pos_)->scn();
    } else {
      Uchar vld = immature::recordVld(curr_record_pos_, ora_version_);

      if (immature::isMajor(vld)) {
        change_length = record_len - constants::kMinMajRecordLen;
        change_pos = curr_record_pos_ + constants::kMinMajRecordLen;
        record_scn = As<RecordHeaderMajor>(curr_record_pos_)->scn();
        epoch_ = As<RecordHeaderMajor>(curr_record_pos_)->getEpoch();
      } else if (immature::isMinor(vld)) {
        change_length = record_len - constants::kMinRecordLen;
        change_pos = curr_record_pos_ + constants::kMinRecordLen;
        record_scn = As<RecordHeaderMinor>(curr_record_pos_)->scn();
      } else if (vld == 0) {
        curr_record_pos_ = nextRecord(curr_record_pos_);
        goto again;
      } else {
        int ivld = vld;
        LOG(ERROR) << "unsupport vld " << ivld << " offset "
                   << curr_record_pos_ - file_start_pos_ << std::endl;
        curr_record_pos_ = nextRecord(curr_record_pos_);
        goto again;
      }
    }

    if (change_length == 0 || record_scn < start_scn_) {
      curr_record_pos_ = nextRecord(curr_record_pos_);
      goto again;
    }

    if (spaceLeft(change_pos) == block_size_)
      change_pos += constants::kBlockHeaderSize;
    size_t offset = curr_record_pos_ - file_start_pos_;
    char* change_buf = new char[change_length];
    realCopyNBytes(change_pos, change_buf, change_length);
    if (isOverWrite()) {
      delete[] change_buf;
      init(log_generator_(log_sequence_).c_str());
      curr_record_pos_ = file_start_pos_ + offset;
      goto again;
    }

    record_scn.noffset_ = offset;
    // TODO: remove noffset in record_buf since we can knows this from SCN
    RecordBufPtr record_buf(new RecordBuf(record_scn, change_length, epoch_,
                                          change_buf, offset, allop_));

    curr_record_pos_ = nextRecord(curr_record_pos_);  // for next round
    return record_buf;
  }

  bool RedoFile::isValid(char* p_record) {
    if (getBlockNo(p_record) > last_block_id_) return false;

    uint32_t left = spaceLeft((char*)p_record);
    if (left < constants::kMinRecordLen) {
      // std::cout << "Invalid :left " << left
      //        << " bytes, can't hold Record_Header_Mintor, size"
      //        << constants::kMinRecordLen << "\t";
      return false;
    }

    if (immature::recordLength(p_record, ora_version_) <
        constants::kMinRecordLen) {
      // std::cout << "Invalid len: "
      //          << immature::recordLength(p_record, ora_version_)
      //           << " Record_header will take " << constants::kMinRecordLen
      //          << " bytes at least \t ";
      return false;
    }

    SCN scn = immature::recordSCN(p_record, ora_version_);
    if (!(scn < p_redo_header_->nextScn()) || scn < lowscn_) {
      std::cout << "Invalid scn " << scn.toStr() << std::endl;
      return false;
    }

    return true;
  }

  Ushort RedoFile::recordOffset(char* p) {
    Ushort offset = 0;
    if (ora_version_ == 9)
      offset = As<BlockHeaderV9>(p)->getFirstOffset();
    else
      offset = As<BlockHeaderV10>(p)->getFirstOffset();
    return offset;
  }

  char* RedoFile::nextValid(char* pos, char* from) {
    bool online_log = false;
  tryagain:
    pos = realAdvanceNBytes(from, immature::recordLength(from, ora_version_));
    if (isOverWrite()) {
      pos = resetPosition(pos);
    }
    while (!isValid(pos)) {
      uint32_t blk_id = getBlockNo(pos);
      // std::cout << "Block id " << getBlockNo(pos) << " offset "
      //        << block_size_ - spaceLeft(pos) << std::endl;

      if (blk_id == last_block_id_) {
        // last block already
        return NULL;
      }

      if (p_redo_header_->next_scn_minor_ == 0xFFFFFFFF &&
          p_redo_header_->next_scn_major_ == 0xFFFF) {
        online_log = true;
        if (isOverRead(blk_id)) {
          LOG(DEBUG) << "blocking on the last block of online log"
                     << "blk_id " << blk_id << " latest_blk " << latest_blk_
                     << std::endl;
          sleep(3);
          pos = from;
          goto tryagain;
        }
      } else {
        if (online_log) {
          pos = resetPosition(pos);
          blk_id = getBlockNo(pos);
        }
      }

      if (blk_id > last_block_id_) {
        /*
         std::cout
             << "impossible!!! the end of last record is out of this file, "
             << "last record information " << std::endl
             << "block : " << getBlockNo(curr_record_pos_)
             << " offset : " << block_size_ - spaceLeft(p_record)
             << "length : " << immature::recordLength(p_record, ora_version_)
             << std::endl;
        */
        return NULL;
      }

      // std::cout << "skip to next block \n";
      char* block_addr = (blk_id + 1) * block_size_ + file_start_pos_;
      pos = block_addr + recordOffset(block_addr);
    }
    return pos;
  }

  char* RedoFile::firstRecord() {
    char* firstblock = file_start_pos_ + 2 * block_size_;
    Ushort offset = recordOffset(firstblock);
    return (file_start_pos_ + 2 * block_size_ + offset);
  }

  char* RedoFile::_realAdvanceNBytes(char* p_record, size_t bytes_count) {
    dassert("p_record must be the begining of a block",
            (p_record - file_start_pos_) % block_size_ == 0);
    size_t block_data_size = block_size_ - constants::kBlockHeaderSize;
    return p_record + (bytes_count / block_data_size) * block_size_ +
           bytes_count % block_data_size + constants::kBlockHeaderSize;
  }

  char* RedoFile::realAdvanceNBytes(char* p_record, size_t bytes_count) {
    size_t left = spaceLeft(p_record);
    if (bytes_count < left) return p_record + bytes_count;

    if (bytes_count == left)
      return p_record + bytes_count + constants::kBlockHeaderSize;

    return _realAdvanceNBytes(p_record + left, bytes_count - left);
  }

  // p_record is a valid record
  char* RedoFile::nextRecord(char* p_record) {
    char* pos = realAdvanceNBytes(
        p_record, immature::recordLength(p_record, ora_version_));
    return nextValid(pos, p_record);
  }
}
