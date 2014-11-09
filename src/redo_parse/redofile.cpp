#include "redofile.h"
#include "physical_elems.h"
#include "util/dassert.h"

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

namespace databus {
  using util::dassert;

  RedoFile::RedoFile(const char* filename) {
    int fd = open(filename, O_RDONLY, 0644);

    dassert(strerror(errno), fd != -1, errno);

    struct stat stats;
    dassert(strerror(errno), fstat(fd, &stats) == 0, errno);
    length_ = stats.st_size;
    file_start_pos_ = (char*)mmap(0, length_, PROT_READ, MAP_SHARED, fd, 0);
    dassert(strerror(errno), file_start_pos_ != NULL, errno);
    close(fd);

    ora_version_ = immature::oracleVersion(file_start_pos_);

    switch (ora_version_) {
      case 9:
        block_size_ = As<FileHeaderV9>(file_start_pos_)->getBlockSize();
        last_block_id_ = As<FileHeaderV9>(file_start_pos_)->getLastBlockID();
        break;
      case 10:
      case 11:
        block_size_ = As<FileHeaderV10>(file_start_pos_)->getBlockSize();
        last_block_id_ = As<FileHeaderV10>(file_start_pos_)->getLastBlockID();
        break;
      default:
        immature::notTestedVersionDie(ora_version_);
    }

    RedoHeader* p_redo_header = As<RedoHeader>(file_start_pos_ + block_size_ +
                                               constants::kBlockHeaderSize);
    lowscn_ = p_redo_header->lowScn();
    nextscn_ = p_redo_header->nextScn();
    db_name_ = std::string(p_redo_header->DbName(), 8);
    db_name_.append(1, '\0');
  }

  RedoFile::~RedoFile() {
    if (file_start_pos_ != NULL) munmap(file_start_pos_, length_);
  }

  bool RedoFile::isValid(const char* p_record) const {
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
    if (!(scn < nextscn_) || scn < lowscn_) {
      std::cout << "Invalid scn " << scn.toStr() << std::endl;
      return false;
    }

    return true;
  }

  Ushort RedoFile::recordOffset(const char* p) const {
    Ushort offset = 0;
    if (ora_version_ == 9)
      offset = As<BlockHeaderV9>(p)->getFirstOffset();
    else
      offset = As<BlockHeaderV10>(p)->getFirstOffset();
    return offset;
  }

  const char* RedoFile::firstRecord() const {
    char* firstblock = file_start_pos_ + 2 * block_size_;
    Ushort offset = recordOffset(firstblock);
    return (char*)(file_start_pos_ + 2 * block_size_ + offset);
  }

  const char* RedoFile::_realAdvanceNBytes(const char* p_record,
                                           size_t bytes_count) const {
    dassert("p_record must be the begining of a block",
            (p_record - file_start_pos_) % block_size_ == 0);
    size_t block_data_size = block_size_ - constants::kBlockHeaderSize;
    return p_record + (bytes_count / block_data_size) * block_size_ +
           bytes_count % block_data_size + constants::kBlockHeaderSize;
  }

  const char* RedoFile::realAdvanceNBytes(const char* p_record,
                                          size_t bytes_count) const {
    size_t left = spaceLeft(p_record);
    if (bytes_count < left) return p_record + bytes_count;

    if (bytes_count == left)
      return p_record + bytes_count + constants::kBlockHeaderSize;

    return _realAdvanceNBytes(p_record + left, bytes_count - left);
  }

  const char* RedoFile::nextRecord(const char* p_record) const {
    const char* pos = realAdvanceNBytes(
        p_record, immature::recordLength(p_record, ora_version_));
    while (!isValid(pos)) {
      // std::cout << "Block id " << getBlockNo(pos) << " offset "
      //        << block_size_ - spaceLeft(pos) << std::endl;
      if (getBlockNo(pos) == last_block_id_) {
        // last block already
        return NULL;
      }

      if (getBlockNo(pos) > last_block_id_) {
        std::cout
            << "impossible!!! the end of last record is out of this file, "
            << "last record information " << std::endl
            << "block : " << getBlockNo(p_record)
            << " offset : " << block_size_ - spaceLeft(p_record)
            << "length : " << immature::recordLength(p_record, ora_version_)
            << std::endl;
        return NULL;
      }

      // std::cout << "skip to next block \n";
      char* block_addr = (getBlockNo(pos) + 1) * block_size_ + file_start_pos_;
      pos = block_addr + recordOffset(block_addr);
    }

    return pos;
  }
}
