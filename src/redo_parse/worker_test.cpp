#include <iostream>
#include <vector>
#include <list>
#include "util/container.h"
#include "workers.h"
#include "stream.h"
#include "metadata.h"
#include "tconvert.h"

namespace databus {

  int main(int ac, char** av) {
    initStream(ac, av);
    Reader redo_reader(&buffers::record_start_positions);
    redo_reader.feed(streamconf->getString("archivelog").c_str());
    redo_reader.srun();
    // std::cout << "found " << buffers::record_start_positions.size() - 1
    //        << " records" << std::endl;

    RecordBuilder record_builder(&redo_reader, &buffers::record_buffer_list);
    /*
    record_builder.setFilter([&metadata](uint32_t x) -> bool {
      return metadata->deserveCapture(x);
    });
    */
    record_builder.srun();

    TransactionBuilder trans_builder(&buffers::record_buffer_list,
                                     &buffers::transaction_list);
    trans_builder.setHandler(tranDump);
    trans_builder.srun();

    return 0;
  }
}

int main(int argc, char** argv) { return databus::main(argc, argv); }
