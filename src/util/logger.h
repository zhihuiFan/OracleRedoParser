#include <sstream>
#include <string>
#include <iostream>
#include "easylogging++.h"

namespace databus {
  inline std::ostream& trace() { return std::cout << "[TRACE] "; }

  inline std::ostream& debug() { return std::cout << "[DEBUG] "; }

  inline std::ostream& info() { return std::cout << "[INFO] "; }

  inline std::ostream& warn() { return std::cout << "[WARN] "; }

  inline std::ostream& error() { return std::cout << "[ERROR] "; }
}
