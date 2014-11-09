#include <list>
#include <string>
#include <sstream>
#include <string.h>
#include <occi.h>

#include "logical_elems.h"
#include "metadata.h"
#include "stream.h"
#include "util/logger.h"

namespace databus {
  using namespace oracle::occi;
  static Number numberAsStr(const char* input, uint32_t len) {
    Number num(0);
    Number base(100);
    Uchar highest = (Uchar)input[0];
    if (highest == 0x80) return num;
    if (highest > 192) {  // positive
      for (int i = 1; i < len; ++i) {
        num += Number(((Uchar)input[i] - 1)) *
               (base.power((Uchar)input[0] - 192 - i));
      }
    } else {  // negative
      for (int i = 1; i < len - 1; ++i) {
        num += Number(((Uchar)input[i] - 101)) *
               (base.power(63 - (Uchar)input[0] - i));
      }
    }
    return num;
  }

  struct OracleDate {
    unsigned char century_;
    unsigned char decade_;
    unsigned char month_;
    unsigned char day_;
    unsigned char hour_;
    unsigned char minute_;
    unsigned char second_;
    unsigned char nanosecond_[4];
    unsigned char zone_hour_;
    unsigned char zone_minute_;
  };

  static const std::string dateToStr(const char* input, uint32_t len) {
    if (len == 0) return "";
    struct OracleDate* ora_date = (struct OracleDate*)input;
    if (len == 7) {  // Only handle date today
      int year, month, day;
      if (ora_date->century_ < 100)
        year = (100 - ora_date->century_) * 100;
      else
        year = (ora_date->century_ - 100) * 100;

      if (ora_date->decade_ < 100)
        year += 100 - ora_date->decade_;
      else
        year += ora_date->decade_ - 100;

      if (ora_date->decade_ < 100 || ora_date->century_ < 100) year = -year;

      std::stringstream ss;
      ss << year << "-" << (int)ora_date->month_ << "-" << (int)ora_date->day_
         << " " << (int)ora_date->hour_ << ":" << (int)ora_date->minute_ << ":"
         << (int)ora_date->second_;
      return ss.str();
    }
    return NULL;
  }

  static const char* convert(const char* input, std::string& type,
                             uint32_t len) {
    // TODO: why length is 0
    if (len == 0) return NULL;
    if (type == "VARCHAR2") return input;
    if (type == "NUMBER") {
      return numberAsStr(input, len)
          .toText(metadata->getEnv(), "999,999,999,999,999,999.99999")
          .c_str();
    }
    if (type == "DATE") {
      return dateToStr(input, len).c_str();
    }
    return NULL;
  }

  void tranDump(XID xid, uint32_t object_id, const char* optype,
                std::list<Row> undos, std::list<Row> redos) {
    if (object_id == 73864) {
      std::cout << 73864 << std::endl;
    }
    TabDef* table_def = metadata->getTabDefFromId(object_id);
    if (table_def == NULL) return;
    BOOST_LOG_TRIVIAL(fatal) << std::endl << std::endl << "Transaction ID "
                             << xid << std::endl;
    BOOST_LOG_TRIVIAL(fatal) << optype << " " << table_def->name << std::endl;
    if (strncmp(optype, "insert", strlen("insert")) != 0) {
      BOOST_LOG_TRIVIAL(fatal) << "Primary Keys:";
      for (auto undo : undos) {
        for (auto col : undo) {
          if (col->len_ > 0 &&
              table_def->pk.find(col->col_id_ + 1) != table_def->pk.end()) {
            BOOST_LOG_TRIVIAL(fatal)
                << "\t" << table_def->col_names[col->col_id_ + 1] << "----"
                << convert(col->content_,
                           table_def->col_types[col->col_id_ + 1], col->len_);
          }
        }
      }
      BOOST_LOG_TRIVIAL(fatal) << std::endl;
    }

    if (strncmp(optype, "delete", strlen("delete")) != 0) {
      BOOST_LOG_TRIVIAL(fatal) << "New data: " << std::endl;
      for (auto redo : redos) {
        for (auto col : redo) {
          BOOST_LOG_TRIVIAL(fatal)
              << table_def->col_names[col->col_id_ + 1] << "----"
              << convert(col->content_, table_def->col_types[col->col_id_ + 1],
                         col->len_) << std::endl;
        }
      }
    }
  }
}
