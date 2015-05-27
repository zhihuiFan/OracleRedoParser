#include <boost/program_options.hpp>
#include <string>
#include <algorithm>
#include <memory>
#include <iostream>

#include "util/dassert.h"
#include "util/logger.h"
#include "stream.h"
#include "metadata.h"
#include "applier.h"
#include "monitor.h"

namespace databus {
  StreamConf* streamconf;
  std::list<std::string> captual_tables;

  static void normalFile(std::ifstream& inf, std::stringstream& ss) {
    std::string s;
    const int max_line_size = 1024;
    char line[1024];
    while (inf) {
      inf.getline(line, max_line_size);
      s = line;
      s.erase(std::remove(s.begin(), s.end(), ' '), s.end());
      s.erase(std::remove(s.begin(), s.end(), '\t'), s.end());
      if (s.c_str()[0] == '#') continue;
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

  static std::list<std::string> initCaptualTable(const char* filename) {
    std::list<std::string> cap_tables;
    std::stringstream ss;
    std::ifstream inf(filename);
    util::dassert("Can't open read from config file", inf.is_open());
    normalFile(inf, ss);
    std::string table;
    while (ss) {
      std::getline(ss, table);
      cap_tables.push_back(table);
    }
    return cap_tables;
  }

  void initStream(int ac, char** av) {
    streamconf = new StreamConf(ac, av);
    std::stringstream ss;
    ss << "logging_" << streamconf->getUint32("instId") << ".conf";
    el::Configurations conf(ss.str().c_str());
    el::Loggers::reconfigureLogger("default", conf);
    captual_tables =
        initCaptualTable(streamconf->getString("tableConf").c_str());
    logmanager = std::shared_ptr<LogManager>(
        new LogManager(streamconf->getString("srcConn").c_str()));
    for (auto table : captual_tables) {
      auto first = table.find_first_of('.');
      auto last = table.find_last_of('.');
      if (first == last && first != table.npos) {
        std::string owner = table.substr(0, first);
        std::string tablename = table.substr(first + 1, table.npos - first - 1);
        LOG(INFO) << "Init table " << table;
        auto tab_def =
            getMetadata().initTabDefFromName(owner.c_str(), tablename.c_str());
        if (tab_def != NULL) {
          LOG(DEBUG) << " init tab def " << tab_def->toString();
          SimpleApplier::getApplier(streamconf->getString("tarConn").c_str())
              .addTable(tab_def);
        }
        // if (tabdef) tabdef->dump();
      } else {
        if (!table.empty())
          LOG(WARNING) << "Invalid Table format " << table
                       << ", The correct format is owner.table_name";
      }
    }
  }
}
