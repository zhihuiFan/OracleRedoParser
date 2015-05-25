#ifndef UTILS_INC
#define UTILS_INC
#include <thread>

namespace util {
  struct guarded_thread : std::thread {
    using thread::thread;
    ~guarded_thread() {
      if (joinable()) join();
    }
  };
}
#endif /* ----- #ifndef UTILS_INC  ----- */
