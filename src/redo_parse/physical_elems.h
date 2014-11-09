#ifndef PHYSICAL_ELEMS_INC
#define PHYSICAL_ELEMS_INC
#include <stdint.h>
#include <set>
#include <string>
#include <sstream>
#include "util/dassert.h"
#include "util/dtypes.h"
#include "logical_elems.h"

namespace databus {
  using util::dassert;
  template <typename T>
  T *As(const char *pointer) {
    return (T *)pointer;
  }

  namespace constants {
    const size_t kBlockHeaderSize = 16;
  }

  class FileHeaderV9 {
   public:
    uint32_t getBlockSize() const { return block_size_; }
    // if there are 100 block in physical, the last_block_id_
    // here is 99
    uint32_t getLastBlockID() const { return last_block_id_; }

   private:
    Uchar zero_;
    Uchar type_;
    Ushort zero1_;
    uint32_t block_size_;
    uint32_t last_block_id_;
    Uchar reserved[500];
  };

  class FileHeaderV10 {
   public:
    uint32_t getBlockSize() const { return block_size_; }
    uint32_t getLastBlockID() const { return last_block_id_; }

   private:
    Uchar unknow_[16];
    Uchar zero_;
    Uchar type_;  // online? archived?
    Ushort zero1_;
    uint32_t block_size_;
    uint32_t last_block_id_;
    Uchar reserved_[484];
  };

  class BlockHeaderV9 {
   public:
    uint32_t getSequenceNum() { return sequence_no_; }
    uint32_t getBlockId() { return block_id_; }
    Ushort getFirstOffset() { return first_record_offset_; }

   private:
    uint32_t sequence_no_;
    uint32_t block_id_;
    uint32_t epoch_;
    Ushort first_record_offset_;
    Ushort r_;
  };

  class BlockHeaderV10 {
   public:
    uint32_t getSequenceNum() { return sequence_no_; }
    uint32_t getBlockId() { return block_id_; }
    Ushort getFirstOffset() { return first_record_offset_ & 0x7FFF; }

   private:
    uint32_t block_type_;  // unknow
    uint32_t block_id_;
    uint32_t sequence_no_;
    Ushort first_record_offset_;
    Ushort r_;
  };

  class RedoHeader {
   public:
    const char *DbName() { return db_name_; }
    uint32_t fileSize() { return file_size_; }
    uint32_t blockSize() { return block_size_; }
    SCN lowScn() { return SCN(low_scn_major_, low_scn_minor_); }
    SCN nextScn() { return SCN(next_scn_major_, next_scn_minor_); }
    Uchar getOraVersion() { return compatible_version_ >> 24; }

   private:
    uint32_t db_version_;  // don't use this one
    uint32_t compatible_version_;
    uint32_t db_id_;
    char db_name_[8];
    uint32_t control_sequence_;
    uint32_t file_size_;
    uint32_t block_size_;
    Ushort file_number_;
    Ushort file_type_;
    uint32_t activation_id_;
    Uchar empty_38h_[0x24];
    char description_[0x40];
    uint32_t nab_;
    uint32_t reset_log_count_;
    uint32_t scn_minor_;
    // The following two fields may be a single 32 bit field,
    // this looks like the case on a bigendian dump.
    Ushort scn_major_;
    Uchar empty_aah_[2];
    uint32_t hws_;
    Ushort thread_;
    Uchar empty_b2h_[2];

    uint32_t low_scn_minor_;
    Ushort low_scn_major_;
    Uchar empty_bah_[2];
    uint32_t low_scn_epoch_;

    uint32_t next_scn_minor_;
    Ushort next_scn_major_;
    Uchar empty_c6h_[2];
    uint32_t next_scn_epoch_;

    Uchar eot_;
    Uchar dis_;
    Uchar empty_ceh_[2];

    uint32_t enabled_scn_minor_;
    Ushort enabled_scn_major_;
    Uchar empty_d6h_[2];
    uint32_t enabled_scn_epoch_;

    uint32_t thread_closed_scn_minor_;
    Ushort thread_closed_scn_major_;
    Uchar empty_e2h_[2];
    uint32_t thread_closed_scn_epoch_;

    uint32_t log_format_version_;
    uint32_t flags_;

    uint32_t terminal_scn_minor_;
    Ushort terminal_scn_major_;
    Uchar empty_f6h_[2];
    uint32_t terminal_epoch_;
    Uchar unused_[260];
  };

  class RecordHeaderV9 {
   public:
    SCN scn() { return SCN(scn_major_, scn_minor_, subscn_); }

   private:
    uint32_t record_len_;
    Uchar vld;
    Uchar subscn_;
    Ushort scn_major_;
    uint32_t scn_minor_;
  };

  class RecordHeaderMinor {
    // total lenght: 22
   public:
    SCN scn() { return SCN(scn_major_, scn_minor_, subscn_); }

   private:
    uint32_t record_length;
    Uchar vld_;
    Uchar foo_;
    Ushort scn_major_;
    uint32_t scn_minor_;
    Ushort subscn_;
    Ushort unknown_1;
    Ushort padding_1;
    uint32_t padding_2;
  };

  class RecordHeaderMajor {
   public:
    SCN scn() { return SCN(scn_major_, scn_minor_, subscn_); }
    uint32_t getEpoch() { return epoch_; }

   private:
    uint32_t record_length_;
    Uchar vld_;
    Uchar foo1_;
    Ushort scn_major_;
    uint32_t scn_minor_;
    Ushort subscn_;
    Ushort unknown_1_;
    Ushort foo2_;
    int unknown2_[10];
    int thread_;
    uint32_t epoch_;
  };

  class ChangeHeader {
   public:
    Ushort opCode() const { return op_major_ << 8 | op_minor_; };

    uint32_t dba() const { return dba_; }
    Uchar type() const { return type_; }

    // offset {headerSize()} will be the start of opCode_xxxx
    uint32_t headerSize() const { return sizeof(ChangeHeader) + align4(lol()); }
    // size of opCode_xxxx
    uint32_t changeSize() const;

    // return the pointer of the part_n
    const char *part(int part_no) const;
    Ushort *partLen(int part_no) const;

   private:
    Ushort lol() const { return *(Ushort *)length_; }  // length of length
    uint32_t align4(uint32_t n) const {
      if (n % 4 == 0) return n;
      return (n / 4 + 1) * 4;
    };
    int partCount() const { return lol() / 2 - 1; }

   public:
    Uchar op_major_;
    Uchar op_minor_;
    Ushort block_class_;
    uint32_t file_id_;
    uint32_t dba_;
    uint32_t low_scn_;
    Ushort high_scn_;
    Uchar zero_[2];
    Uchar seq_;
    Uchar type_;
    Ushort zero2;
    char length_[0];  // array of length
  };                  // ChangeHeader

  namespace immature {
    const std::set<Uchar> major_vlds{0x4, 0x5, 0x6, 0x9, 0xd};
    const std::set<Uchar> minor_vlds{0x1, 0x2};

    extern inline bool isMajor(Uchar vld) {
      return major_vlds.find(vld) != major_vlds.end();
    }

    extern inline bool isMinor(Uchar vld) {
      return minor_vlds.find(vld) != minor_vlds.end();
    }

    extern inline bool testedVersion(Uchar ora_version) {
      return ora_version > 8 && ora_version < 12;
    }

    extern inline void notTestedVersionDie(Uchar ora_version) {
      std::stringstream ss;
      int v = ora_version;
      ss << "Only 9i/10g/11g supported, but found version " << v;
      dassert(ss.str().c_str(), false);
    }

    extern inline uint32_t recordLength(const char *p_record,
                                        Uchar ora_version) {
      if (testedVersion(ora_version))
        return *((uint32_t *)p_record);
      else {
        notTestedVersionDie(ora_version);
        return 0;
      }
    }

    extern inline Uchar recordVld(const char *p_record, Uchar ora_version) {
      if (testedVersion(ora_version))
        return *((Uchar *)(p_record + sizeof(int)));
      else {
        notTestedVersionDie(ora_version);
        return 0;
      }
    }

    extern inline SCN recordSCN(const char *p_record, Uchar version) {
      switch (version) {
        case 9:
          return As<RecordHeaderV9>(p_record)->scn();
        case 10:
        case 11:
          return As<RecordHeaderMinor>(p_record)->scn();
        default: {
          notTestedVersionDie(version);
          return SCN(0, 0);
        }
      }
    }

    extern inline Uchar oracleVersion(const char *p_logfile) {
      return As<RedoHeader>(p_logfile + 512 + constants::kBlockHeaderSize)
          ->getOraVersion();
    };
  }

  namespace constants {
    const size_t kMinRecordLen = sizeof(RecordHeaderMinor);
    const size_t kMinMajRecordLen = sizeof(RecordHeaderMajor);
    const size_t kChangeHeaderSize = sizeof(ChangeHeader);
  }
}

#endif /* ----- #ifndef PHYSICAL_ELEMS_INC  ----- */
