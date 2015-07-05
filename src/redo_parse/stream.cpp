#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include <algorithm>
#include <memory>
#include <iostream>
#include <unistd.h>

#include "util/dassert.h"
#include "util/logger.h"
#include "stream.h"
#include "metadata.h"
#include "applier.h"
#include "monitor.h"
#include "redofile.h"

namespace databus {
  StreamConf* streamconf;

  static void normalFile(std::ifstream& inf, std::stringstream& ss) {
    std::string s;
    const int max_line_size = 1024;
    char line[1024];
    while (inf) {
      inf.getline(line, max_line_size);
      s = line;
      boost::trim(s);
      if (boost::starts_with(s, "#")) continue;
      if (!s.empty()) ss << s << std::endl;
    }
  }

  StreamConf::StreamConf(int argc, char** argv) : desc("Supported Options") {
    add_options();
    po::store(po::parse_command_line(argc, argv, desc), vm);
    if (vm.count("confFile")) {
      std::ifstream inf(vm["confFile"].as<std::string>().c_str());
      util::dassert("Can't open read from config file", inf.is_open());
      std::stringstream ss;
      normalFile(inf, ss);
      po::store(po::parse_config_file(ss, desc), vm);
    }
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      std::exit(1);
    }
    po::notify(vm);
    validParams();
  }

  void StreamConf::add_options() {
    desc.add_options()("help,h", "show help message")(
        "srcConn", po::value<std::string>(),
        "string for source database, format username/password@vm")(
        "tarConn", po::value<std::string>(),
        "string for target database username/password@vm")(
        "instId", po::value<uint32_t>(), "DataStream instance Id")(
        "confFile", po::value<std::string>(), "configure file for data stream")(
        "tableConf", po::value<std::string>(), "tables to capture changes")(
        "startSeq", po::value<uint32_t>(),
        "the log sequence to start with, will be removed soon");
  }

  void StreamConf::validParams() {
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      std::exit(1);
    }
    util::dassert("srcConn/tarConn/tableConf are must",
                  vm.count("srcConn") && vm.count("tarConn") &&
                      vm.count("tableConf") && vm.count("instId"));
  }

  int StreamConf::getInt(const char* para, int default_value) {
    if (vm.count(para)) return vm[para].as<int>();
    return default_value;
  }

  uint32_t StreamConf::getUint32(const char* para) {
    return vm[para].as<uint32_t>();
  }

  std::string StreamConf::getString(const char* para,
                                    const char* default_value) {
    if (vm.count(para)) return vm[para].as<std::string>();
    return default_value;
  }

  bool StreamConf::getBool(const char* para, bool default_value) {
    if (vm.count(para)) return vm[para].as<bool>();
    return default_value;
  }

  static std::list<TabConf> initCaptualTable(const char* filename) {
    std::list<TabConf> cap_tables;
    std::stringstream ss;
    std::ifstream inf(filename);
    util::dassert("Can't open read from config file", inf.is_open());
    normalFile(inf, ss);
    std::string line;
    std::vector<std::string> v(2);
    while (ss) {
      std::getline(ss, line);
      boost::trim(line);
      if (boost::starts_with(line, "#")) {
        continue;
      }
      boost::split(v, line, boost::is_any_of("#"));
      switch (v.size()) {
        case 1:
          cap_tables.push_back(TabConf(boost::trim_copy(v[0])));
          break;
        case 2:
          cap_tables.push_back(
              TabConf(boost::trim_copy(v[0]), boost::trim_copy(v[1])));
          break;
        default:
          LOG(ERROR) << "bad format of tabconf " << line;
      }
    }
    return cap_tables;
  }

  void initStream(int ac, char** av) {
    streamconf = new StreamConf(ac, av);
    std::stringstream ss;
    ss << "logging_" << streamconf->getUint32("instId") << ".conf";
    el::Configurations conf(ss.str().c_str());
    el::Loggers::reconfigureLogger("default", conf);
    auto captual_tables =
        initCaptualTable(streamconf->getString("tableConf").c_str());
    for (auto table_conf : captual_tables) {
      auto first = table_conf.tab_name.find_first_of('.');
      auto last = table_conf.tab_name.find_last_of('.');
      if (first == last && first != table_conf.tab_name.npos) {
        std::string owner = table_conf.tab_name.substr(0, first);
        std::string tablename = table_conf.tab_name.substr(
            first + 1, table_conf.tab_name.npos - first - 1);
        LOG(INFO) << "Init table " << table_conf.tab_name;
        auto tab_def =
            getMetadata().initTabDefFromName(owner.c_str(), tablename.c_str());
        if (tab_def != NULL) {
          LOG(DEBUG) << " init tab def " << tab_def->toString();
          SimpleApplier::getApplier(streamconf->getString("tarConn").c_str())
              .addTable(tab_def, table_conf);
        }
        // if (tabdef) tabdef->dump();
      } else {
        if (!table_conf.tab_name.empty())
          LOG(WARNING) << "Invalid Table format " << table_conf.tab_name
                       << ", The correct format is owner.table_name";
      }
    }
  }

  std::string getLogfile(uint32_t seq) {
    return getLogManager().getLogfile(seq);
  }

  uint32_t getOnlineLastBlock(uint32_t seq) {
    return getLogManager().getOnlineLastBlock(seq);
  }

  void startMining(uint32_t seq, const TimePoint& tm) {
    while (true) {
      if (seq - GlobalStream::getGlobalStream().getAppliedSeq() > 2) {
        LOG(DEBUG) << "Sleep 3 seconds, applied seq is "
                   << GlobalStream::getGlobalStream().getAppliedSeq();
        sleep(3);
        continue;
      }
      RedoFile redofile(seq, getLogfile, getOnlineLastBlock);
      redofile.setStartScn(tm.scn_);
      RecordBufPtr buf;
      unsigned long c = 0;
      while ((buf = redofile.nextRecordBuf()).get()) {
        if (buf->change_vectors.empty()) continue;
        getRecordBufList().push_back(buf);
      }
      ++seq;
    }
  }

  /*
  void applyRecordBuf() {
    uint32_t curr_seq = GlobalStream::getGlobalStream().getAppliedSeq();
    LOG(DEBUG) << "Last applied seq " << curr_seq;
    while ((curr_record_ = getRecordBufList().pop_front()) != NULL) {
      if (curr_seq == curr_record_->seq_) {
        addToTransaction(curr_record_);
      } else {
        LOG(DEBUG) << "Bug seq " << buf->seq_ << " Last applied seq "
                   << curr_seq;
        auto n = Transaction::removeUncompletedTrans();
        if (n > 0)
          LOG(WARNING) << "removed " << n
                       << " incompleted transaction in log seq " << curr_seq;
        LOG(INFO) << "Build Transaction now" << std::endl;
        auto tran = Transaction::xid_map_.begin();
        while (tran != Transaction::xid_map_.end()) {
          auto it = buildTransaction(tran);
          if (it != Transaction::xid_map_.end()) {
            tran = it;
          } else {
            tran++;
          }
        }

        if (!Transaction::start_scn_q_.empty()) {
          auto it = Transaction::start_scn_q_.begin();
          Transaction::setRestartTimePoint(it->first, it->second);
        }

        LOG(INFO) << "Apply Transaction now, Total  "
                  << Transaction::commit_trans_.size() << " to apply "
                  << std::endl;
        auto commit_tran = Transaction::commit_trans_.begin();
        while (commit_tran != Transaction::commit_trans_.end()) {
          SimpleApplier::getApplier(streamconf->getString("tarConn").c_str())
              .apply(commit_tran->second);
          commit_tran = Transaction::commit_trans_.erase(commit_tran);
        }
        GlobalStream::getGlobalStream().setAppliedSeq(++curr_seq);
      }
    }
  }  */

  void startStream(uint32_t seq, const TimePoint& tm) {
    std::thread mining{startMining, seq, tm};
    mining.detach();
    std::thread applier{ApplierManager::getApplierManager()};
    applier.detach();
  }
}
