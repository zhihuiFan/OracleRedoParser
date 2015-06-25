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
#include <atomic>

#include "util/container.h"
#include "metadata.h"
#include "logical_elems.h"
#include "trans.h"

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

  inline MetadataManager& getMetadata() {
    static MetadataManager metadata(streamconf->getString("srcConn").c_str());
    return metadata;
  }

  class GlobalStream {

   public:
    static GlobalStream& getGlobalStream() {
      static GlobalStream global_stats;
      return global_stats;
    }
    uint32_t getAppliedSeq() const { return applied_seq_.load(); }
    void setAppliedSeq(uint32_t seq) { applied_seq_ = seq; }

   private:
    GlobalStream() : applied_seq_(0) {}
    std::atomic_uint applied_seq_;
  };

  inline List<RecordBufPtr>& getRecordBufList() {
    static List<RecordBufPtr> record_buf_list;
    return record_buf_list;
  }

  inline StreamConf& getStreamConf() { return *streamconf; }
  void initStream(int ac, char** av);
  void streamMonitor();
  void startStream(uint32_t seq, const TimePoint& tp);
  void startMining(uint32_t seq, const TimePoint& tp);
  void applyRecordBuf();
  inline std::vector<std::thread>& getGlobalThreads() {
    static std::vector<std::thread> thrs;
    return thrs;
  }

  inline LogManager& getLogManager() {
    static LogManager log_manager(streamconf->getString("srcConn").c_str());
    return log_manager;
  }
}
#endif /* ----- #ifndef REDO_PARSE_INC  ----- */
