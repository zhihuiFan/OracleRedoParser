#ifndef OPCODE_OPS_INC
#define OPCODE_OPS_INC

#include <list>
#include <memory>

#include "opcode.h"
#include "physical_elems.h"
#include "logical_elems.h"
#include "util/dassert.h"
#include "util/dtypes.h"

namespace databus {
  using util::dassert;
  using util::strange;

  bool validOp(Ushort op);

  class Ops0501 {
    // I don't want to build a Ops0501 class per 0501 change, that will cause
    // too many construct and Destruction, that will be too ineffecitve
   public:
    // In c++11, return std::list is effective
    static std::list<Row> makeUpUndo(const ChangeHeader* change0501,
                                     Ushort& uflag_, Ushort& start_col);
    static bool enabledSuppmentalLog(const ChangeHeader* change0501);

    static XID getXID(const ChangeHeader* change0501);
    static uint32_t getObjId(const ChangeHeader* change0501);
    static uint32_t getDataObjId(const ChangeHeader* change0501);

   private:
    // only for test
    static void printTransBase(const ChangeHeader* change0501);
  };

  class OpsDML {
   public:
    static std::list<Row> makeUpRedoCols(const ChangeHeader* change,
                                         Uchar& iflag);
  };

  // TODO:when to relase the heap memory ??
  Row _makeUpUncommCols(const char* start, int total_cols);
  Row _makeUpNoLenPrefixCols(Ushort* col_num, Ushort total_cols,
                             const ChangeHeader* change, Ushort data_offset,
                             bool supplemental = false);
  Row _makeUpLenPrefixCols(Ushort* col_num, Ushort total_cols,
                           const ChangeHeader* change,
                           OpCodeSupplemental** sup);
  Row makeUpCols(Ushort* col_num, Ushort total_cols, const ChangeHeader* change,
                 Ushort data_offset, Ushort xtype, bool supplemental = false);
}
#endif /* ----- #ifndef OPCODE_OPS_INC  ----- */
