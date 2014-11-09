#ifndef TCONVERT_INC
#define TCONVERT_INC
#include "logical_elems.h"
namespace databus {
  void tranDump(XID xid, uint32_t object_id, const char* optype,
                std::list<Row> undos, std::list<Row> redos);
}

#endif /* ----- #ifndef TCONVERT_INC  ----- */
