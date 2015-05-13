#include <boost/algorithm/string.hpp>
#include <list>
#include <string>
#include <sstream>
#include <string.h>
#include <cmath>
// #include <occi.h>

#include "logical_elems.h"
#include "metadata.h"
#include "stream.h"
#include "util/logger.h"

namespace databus {
  // using namespace oracle::occi;
  /*
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
  */

  static std::string numberAsStr(const char* input, uint32_t len) {
    unsigned char sign_bit = *input;  // 193, 64 ...
    if (len == 1) {
      assert(sign_bit == 128);
      return std::string("0");
    }

    std::stringstream ss;
    unsigned short n;
    bool floated = false;

    if (sign_bit > 128) {
      int int_bit = sign_bit - 192;
      if (int_bit < 0) {
        ss << ".";
        for (int i = 0; i < abs(int_bit); ++i) {
          ss << "00";
        }
      }
      for (Uchar i = 1; i < len || i <= int_bit; ++i) {
        if (i > int_bit && int_bit > 0 && !floated) {
          ss << ".";
          floated = true;
        }
        if (i >= len) {
          ss << "00";
        } else {
          n = input[i] - 1;
          if (n < 10) {
            ss << 0;
          }
          ss << n;
        }
      }
    } else {
      ss << "-";
      int int_bit = 63 - sign_bit;
      for (int i = 1; i < len - 1 || i < abs(int_bit) + 1; ++i) {
        // input[len-1] == 102, useless
        if (i > int_bit && !floated) {
          ss << ".";
          floated = true;
          if (int_bit <= 0) {
            // -0.00000001
            for (int i = 0; i < abs(int_bit); ++i) {
              ss << "00";
            }
          }
        }
        if (i >= len - 1) {
          // handle -10000
          ss << "00";
        } else {
          n = 101 - input[i];
          if (n < 10) {
            ss << "0";
          }
          ss << n;
        }
      }
    }
    return ss.str();
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

  std::string convert(const char* input, std::string& type, Ushort len) {
    // TODO: why length is 0 A: NULL ? Confirm: Yes,  TODO: in real case, we
    // need a NULL here
    if (len == 0) return "";
    if (type == "VARCHAR2") {
      return std::string(input, len);
    }
    if (type == "NUMBER") {
      return numberAsStr(input, len);
    }
    if (type == "DATE") {
      return dateToStr(input, len);
    }
    return "";
  }

  void tranDump(XID xid, uint32_t object_id, const char* optype,
                std::list<Row> undos, std::list<Row> redos) {
    auto table_def = getMetadata().getTabDefFromId(object_id);
    if (table_def == NULL) {
      LOG(DEBUG) << "Obj (" << object_id << ") doesn't exist or don't have PK"
                 << std::endl;

      return;
    }
    LOG(INFO) << "Transaction ID " << xid << std::endl;
    LOG(INFO) << optype << " " << table_def->name << std::endl;
    if (strncmp(optype, "insert", strlen("insert")) != 0) {
      LOG(INFO) << "Primary Keys:" << std::endl;
      for (auto undo : undos) {
        for (auto col : undo) {
          if (col->len_ > 0 &&
              table_def->pk.find(col->col_id_ + 1) != table_def->pk.end()) {
            LOG(INFO) << "\t" << table_def->col_names[col->col_id_ + 1]
                      << "----"
                      << convert(col->content_,
                                 table_def->col_types[col->col_id_ + 1],
                                 col->len_) << std::endl;
          }
        }
      }
    }

    if (strncmp(optype, "delete", strlen("delete")) != 0) {
      LOG(INFO) << "New data: " << std::endl;
      for (auto redo : redos) {
        for (auto col : redo) {
          LOG(INFO) << table_def->col_names[col->col_id_ + 1] << "----"
                    << convert(col->content_,
                               table_def->col_types[col->col_id_ + 1],
                               col->len_) << std::endl;
        }
      }
    }
  }
}
