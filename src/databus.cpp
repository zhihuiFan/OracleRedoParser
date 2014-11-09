#include "util/container.h"
#include "redo_parse/logical_elems.h"
#include "redo_parse/redofile.h"
// #include "redo_parse/workers.h"

#include <chrono>
#include <thread>

namespace databus {
  namespace buffers {
    List<const char*> record_start_positions;
    List<RecordBuf*> record_buffer_list;
    List<Transaction*> transaction_list;

    // Monitor buffer usage every 60 seconds
    void ReportBuffUsage() {
      while (true) {
        ReportList(record_start_positions, "RecordPosisions ");
        ReportList(record_buffer_list, "RecordBuffers ");
        ReportList(transaction_list, "TranactionList ");
        std::this_thread::sleep_for(std::chrono::seconds(5));
      }
    }
  }

  int main(int argc, char** argv) {
    // buffers::ReportBuffUsage();
    RedoFile f(argv[1]);
    const char* record = f.firstRecord();
    do {
      buffers::record_start_positions.push_back(record);
    } while ((record = f.nextRecord(record)) != NULL);

    std::cout << "totol record count " << buffers::record_start_positions.size()
              << std::endl;
    for (auto i : f.block_record_count_) {
      std::cout << "block id " << i.first << "count " << i.second << std::endl;
    }
    return 0;
  }
}

int main(int argc, char** argv) { return databus::main(argc, argv); }
