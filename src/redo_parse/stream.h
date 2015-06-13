#ifndef REDO_PARSE_INC
#define REDO_PARSE_INC
#include <string>
#include <boost/program_options.hpp>
#include <fstream>
#include <sstream>
#include <list>
#include <string>
#include <chrono>
#include <thread>
#include <memory>

#include "util/container.h"
#include "metadata.h"

namespace databus {
  namespace po = boost::program_options;
  class StreamConf {
   public:
    StreamConf(int ac, char** av);
    int getInt(const char*, int default_value = -1);
    std::string getString(const char*, const char* default_value = "");
    bool getBool(const char*, bool default_value = false);
    uint32_t getUint32(const char* para);

   private:
    void add_options();
    void validParams();
    void parseConfigFile(std::ifstream& inf, std::stringstream& ss);

   private:
    po::options_description desc;
    po::variables_map vm;
  };

  struct TabConf {
    std::string tab_name;
    std::string tbs_name;
    TabConf(const std::string& tab, const std::string& tbs = "")
        : tab_name(tab), tbs_name(tbs) {}
  };
  extern StreamConf* streamconf;
  extern std::shared_ptr<LogManager> logmanager;

  inline MetadataManager& getMetadata() {
    static MetadataManager metadata(streamconf->getString("srcConn").c_str());
    return metadata;
  }

  inline StreamConf& getStreamConf() { return *streamconf; }

  void initStream(int ac, char** av);

  void streamMonitor();
}

#endif /* ----- #ifndef REDO_PARSE_INC  ----- */
