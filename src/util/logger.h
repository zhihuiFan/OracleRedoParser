#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>

namespace databus {

  namespace logging = boost::log;

  // defined in trivial.hpp
  // enum severity_level
  // {
  //     trace,   0
  //     debug,   1
  //     info,    2
  //     warning, 3
  //     error,   4
  //     fatal    5
  // }

  inline void setLogLevel(short log_level) {
    if (log_level > 5) log_level = 5;
    if (log_level < 0) log_level = 0;
    logging::core::get()->set_filter(logging::trivial::severity >= log_level);
  }
}
