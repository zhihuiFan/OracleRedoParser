/*
 * =====================================================================================
 *
 *       Filename:  redo_parse.h
 *
 *    Description:
 *
 *        Version:  1.0
 *        Created:  08/24/2014 18:23:42
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  zhifan (Zhihui Fan), zhihuifan@163.com
 *   Organization:
 *
 * =====================================================================================
 */

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

#include "util/container.h"
#include "workers.h"
#include "stream.h"
#include "metadata.h"

namespace databus {
  namespace buffers {
    extern List<const char*> record_start_positions;
    extern List<RecordBuf*> record_buffer_list;
    extern List<Transaction*> transaction_list;
  }

  namespace po = boost::program_options;
  class StreamConf {
   public:
    StreamConf(int ac, char** av);
    int getInt(const char*, int default_value = -1);
    std::string getString(const char*, const char* default_value = "");
    bool getBool(const char*, bool default_value = false);

   private:
    void add_options();
    void validParams();
    void parseConfigFile(std::ifstream& inf, std::stringstream& ss);

   private:
    po::options_description desc;
    po::variables_map vm;
  };

  extern MetadataManager* metadata;
  extern StreamConf* streamconf;
  extern std::list<std::string> captual_tables;

  void initStream(int ac, char** av);
}

#endif /* ----- #ifndef REDO_PARSE_INC  ----- */
