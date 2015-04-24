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

    /*
    if (vm.count("loglevel")) {
      setLogLevel(vm["loglevel"].as<unsigned short>());
    } else {
      setLogLevel(3);
    }
    */

    po::notify(vm);
    validParams();
  }

  void StreamConf::add_options() {
    desc.add_options()("help,h", "show help message")(
        "srcUser", po::value<std::string>()->default_value("dbstream"),
        "Username for source oracle instance")(
        "srcPass", po::value<std::string>(),
        "password for source oracle instance")(
        "srcDB", po::value<std::string>(), "connection string for source db")(
        "confFile", po::value<std::string>(), "configure file for data stream")(
        "tableConf", po::value<std::string>(), "tables to capture changes")(
        "startSeq", po::value<uint32_t>(), "the log sequence to start with")(
        "loglevel,v", po::value<unsigned short>(),
        "loger level, the higher the more details, default 3");
  }

  void StreamConf::validParams() {
    if (vm.count("help")) {
      std::cout << desc << std::endl;
      std::exit(1);
    }
    /*
    util::dassert("srcUser/srcPass/srcHost/tableConf are must",
                  vm.count("srcUser") && vm.count("srcPass") &&
                      vm.count("srcDB") && vm.count("tableConf") &&
                      vm.count("startSeq"));
                      */
  };

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
    captual_tables =
        initCaptualTable(streamconf->getString("tableConf").c_str());
    /*
    for (auto i : captual_tables) {
      // TODO: Where is the empty captured table
      // if (i.empty()) std::cout << "emtyt captured " << std::endl;
      std::cout << i << std::endl;
    }
    */
    logmanager = std::shared_ptr<LogManager>(
        new LogManager(streamconf->getString("srcUser").c_str()));
    for (auto table : captual_tables) {
      auto first = table.find_first_of('.');
      auto last = table.find_last_of('.');
      if (first == last && first != table.npos) {
        std::string owner = table.substr(0, first);
        std::string tablename = table.substr(first + 1, table.npos - first - 1);
        auto tab_def =
            getMetadata().initTabDefFromName(owner.c_str(), tablename.c_str());
        if (tab_def != NULL) {
          LOG(DEBUG) << " init tab def " << tab_def->toString();
          SimpleApplier::getApplier(streamconf->getString("srcUser").c_str())
              .addTable(tab_def);
        }
        // if (tabdef) tabdef->dump();
      } else {
        // TODO: why?
        // std::cout << "[warnings ] invalid table " << table << std::endl;
      }
    }
  }
}
