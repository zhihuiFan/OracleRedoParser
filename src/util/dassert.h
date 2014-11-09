#ifndef DASSERT_INC
#define DASSERT_INC
#include <iostream>

namespace util {

  inline void dassert(const char* err, bool expr, int exitno = -1) {
    if (!expr) {
      std::cerr << err << std::endl;
      std::exit(exitno);
    }
  }

  inline void strange(const char* message) {
    std::cout << "[strange] " << message << std::endl;
  }
}
#endif /* ----- #ifndef DASSERT_INC  ----- */
