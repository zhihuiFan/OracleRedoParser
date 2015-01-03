#include <iostream>
#include <vector>
#include <list>
#include <memory>
#include "util/container.h"
#include "stream.h"
#include "metadata.h"
#include "tconvert.h"
#include "redofile.h"
#include "opcode.h"
#include "util/logger.h"
#include "opcode_ops.h"
#include "trans.h"

namespace databus {

  /*
    void handleBuf(RecordBuf* record_buf) {
      XID xid = 0;
      static size_t buf_seq = 0;
      BOOST_LOG_TRIVIAL(debug)
          << " offset = " << std::hex << record_buf->offset() / 512 << ":"
          << record_buf->offset() % 512 << std::dec << "(" << ++buf_seq << ")"
          << std::endl;
      std::list<Row> undo, redo;
      uint32_t object_id;
      uint32_t data_object_id;
      const char* optype = NULL;
      for (auto i : record_buf->change_vectors) {
        switch (i->opCode()) {
          case opcode::kUndo: {
            xid = Ops0501::getXID(i);
            object_id = Ops0501::getObjId(i);
            data_object_id = Ops0501::getDataObjId(i);
            undo = Ops0501::makeUpUndo(i);
            // why some undo is empty? check seq 211
            // if (undo.empty()) return;
          } break;
          case opcode::kUpdate:
            redo = OpsDML::makeUpRedoCols(i);
            optype = "update";
            break;
          case opcode::kInsert:
          case opcode::kMultiInsert:
            if (!redo.empty())
              BOOST_LOG_TRIVIAL(warning) << "redo row is empty " << std::endl;
            redo = OpsDML::makeUpRedoCols(i);
            optype = "insert";
            break;
          case opcode::kDelete:
            optype = "delete";
            break;
        }  // end switch
      }
      if (optype != NULL) tranDump(xid, object_id, optype, undo, redo);
    }
    */

  std::shared_ptr<LogManager> logmanager = NULL;

  int main(int ac, char** av) {
    initStream(ac, av);
    RedoFile redofile(
        getStreamConf().getUint32("startSeq"),
        [](uint32_t seq) -> std::string { return logmanager->getLogfile(seq); },
        [](uint32_t seq)
            -> uint32_t { return logmanager->getOnlineLastBlock(seq); });
    RecordBufPtr buf;
    unsigned long c = 0;
    while ((buf = redofile.nextRecordBuf()) != NULL) {
      buildTransaction(buf);
      ++c;
    }
    BOOST_LOG_TRIVIAL(debug) << " total record found  = " << c;
    BOOST_LOG_TRIVIAL(info) << "Dump Transaction now";
    for (auto tran : Transaction::getXIDMap()) {
      BOOST_LOG_TRIVIAL(info) << tran.second->toString();
    }
    //    MetadataManager::destory();
    return 0;
  }
}

int main(int argc, char** argv) { return databus::main(argc, argv); }
