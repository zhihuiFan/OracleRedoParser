#ifndef OPCODE_INC
#define OPCODE_INC
#include <list>
#include "util/dtypes.h"
#include "logical_elems.h"

namespace databus {

  namespace opcode {
    const Ushort kOther = 0x0005;
    const Ushort kUndo = 0x0501;
    const Ushort kBeginTrans = 0x0502;
    const Ushort kCommit = 0x0504;
    const Ushort kNewSession = 0x0513;
    const Ushort kSwitchSession = 0x0514;
    // Row Operation
    const Ushort kInsert = 0x0b02;
    const Ushort kDelete = 0x0b03;
    const Ushort kLock = 0x0b04;
    const Ushort kUpdate = 0x0b05;
    const Ushort kRowChain = 0x0b06;
    const Ushort kMfc = 0x0b07;
    const Ushort kCfa = 0x0b08;
    const Ushort kCki = 0x0b09;  // Cluster Key Index
    const Ushort kSkl = 0x0b0a;  // Set cluster key pointer
    const Ushort kMultiInsert = 0x0b0b;
    const Ushort kMultiDelete = 0x0b0c;
    const Ushort kLmn = 0x0b10;
    const Ushort kDdl = 0x1801;
    std::map<Ushort, std::string> map{{kInsert, "insert"},
                                      {kUpdate, "Update"},
                                      {kDelete, "Delete"},
                                      {kMultiInsert, "Insert"},
                                      {kRowChain, "Update"}};
  };

  struct OpCode0501 {
    Ushort size_;
    Ushort space_;
    Ushort flag_;
    Ushort unknown1_;
    Ushort xid_high_;
    Ushort xid_mid_;
    uint32_t xid_low_;
    Ushort seq_;
    Uchar rec_;
  };

  struct OpCode0501Sec {
    uint32_t object_id_;
    uint32_t data_object_id_;
    uint32_t tsn_;
    uint32_t unknown1;
    Uchar op_major_;
    Uchar op_minor_;
    Uchar slt_;
    Uchar rci_;
    Uchar is_ktubl_;
  };

  struct OpCodeKdo {
    uint32_t bdba_;  // block address
    uint32_t hdba_;  // segment header block address
    Ushort maxfr_;
    Uchar opcode_;
    Uchar xtype_;
    Uchar itli_;
    Uchar ispac_;
  };

  struct OpCodeKtbdir {
    char opcode_;
    Uchar version_;
    Uchar unknown_[3];
  };

  // insert
  struct OpCodeKdoirp {
    uint32_t bdba_;
    uint32_t hdba_;
    Ushort maxfr_;
    Uchar opcode_;
    Uchar xtype_;
    Uchar itli_;
    Uchar ispac_;
    Ushort unknown1_;
    Uchar flag_;
    Uchar lb_;
    Uchar column_count_;
    Uchar cki_;
    uint32_t hrid_;
    Ushort hrid_minor_;
    Ushort unknown2_;
    uint32_t nrid_;
    Ushort nrid_minor_;
    Ushort unknown3_;
    uint32_t unknown4_;
    Ushort size_;
    Ushort slot_;
    Uchar tabn_;
    Uchar null_bitmap_[1];
  };

  // cfa
  struct OpCodeKdocfa {
    uint32_t bdba_; /* 00h */
    uint32_t hdba_; /* 04h */
    Ushort maxfr_;  /* 08h */
    Uchar opcode_;  /* 0ah */
    Uchar xtype_;   /* 0bh */
    Uchar itli_;    /* 0ch */
    Uchar ispac_;   /* 0dh */
    Ushort unknow_;
    uint32_t nrid_;
    Ushort nrid_minor_;
    Uchar tabn_;
    Ushort slot_;
  };

  // delete
  struct OpCodeKdodrp {
    uint32_t bdba_;
    uint32_t hdba_;
    Ushort maxfr_;
    Uchar opcode_;
    Uchar xtype_;
    Uchar itli_;
    Uchar ispac_;
    Ushort unknown1_;
    Ushort slot_;
    Uchar tabn_;
  };

  // update
  struct OpCodeKdourp {        // part 4
    uint32_t bdba_;            /* 00h */
    uint32_t hdba_;            /* 04h */
    Ushort maxfr_;             /* 08h */
    Uchar opcode_;             /* 0ah */
    Uchar xtype_;              /* 0bh */
    Uchar itli_;               /* 0ch */
    Uchar ispac_;              /* 0dh */
    Ushort unknown1_;          /* 0eh */
    Uchar flag_;               /* 10h */
    Uchar lock_;               /* 11h */
    Uchar ckix_;               /* 12h */
    Uchar tabn_;               /* 13h */
    Ushort slot_;              /* 14h */
    Uchar ncol_; /* 16h */     /* total column number */
    Uchar nchanged_; /* 17h */ /*  changed columes number  */
    short size_; /* 18h */     /* what size (-5) */
    Uchar null_bitmap_[1];     /* 1ah */
  };

  // mfc
  struct OpCodeKdomfc {
    uint32_t bdba_;
    uint32_t hdba_;
    Ushort maxfr_;
    Uchar opcode_;
    Uchar xtype_;
    Uchar itli_;
    Uchar ispac_;
    Uchar unknown1_[2];
    Ushort slot_;
    Uchar tabn_;
  };

  struct OpCodeKdoqm {
    uint32_t bdba_;
    uint32_t hdba_;
    Ushort maxfr_;
    Uchar opcode_;
    Uchar xtype_;
    Uchar itli_;
    Uchar ispac_;
    Ushort unknown1_;
    Uchar tabn_;
    Uchar lock_;
    Uchar nrow_;
    Uchar unknown_;
    Ushort slot_[1];
  };

  struct RedoRid {
    uint32_t major;
    Ushort minor;
  };

  struct OpCodeSupplemental {
    Uchar unknown_;
    Uchar flag_;
    Ushort total_cols_;
    Ushort objv_;
    Ushort start_column_;
    Ushort start_column2_;
  };

  struct OpCode0502 {
    Ushort slot_;
    Ushort unknow_;
    uint32_t sqn_;
    uint32_t uba_high_;
    Ushort uba_mid_;
    Uchar uba_low_;     /* 0x2e */
    Uchar unknown6_[1]; /* 0x2f */
    Ushort flag_;       /* 0x30 */
    Ushort size_;       /* 0x32 */
    Uchar fbi_;         /* 0x34 */
    Uchar unknown1_;
    Ushort unknown2_;
    Ushort pxid_major_;
    Ushort pxid_minor_;
    uint32_t pxid_micro_;
  };

  struct OpCode0504_ucm {
    Ushort slt_;
    Ushort unknown2;
    uint32_t sqn_;
    Uchar srt;
    Uchar unknown1[3];
    uint32_t sta;
    Uchar flg_;
  };
  struct OpCode0504_ucf {
    uint32_t uba_blk_;
    Ushort uba_seq_;
    Uchar uba_slt_;
    Uchar padding_;
    Ushort ext_;
    Ushort spc_;
    Uchar fbi_;
    Uchar unknown_[3];
  };
}

#endif /* ----- #ifndef OPCODE_INC  ----- */
