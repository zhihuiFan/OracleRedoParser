#ifndef METADATA_INC
#define METADATA_INC
#ifndef OTL_ORA11G_R2
#define OTL_ORA11G_R2
#endif
#include <string>
#include <stdlib.h>
#include <map>
#include <list>
#include <set>
#include <vector>
#include <assert.h>
#include <sstream>
#include <iostream>
#include <memory>
#include "util/dtypes.h"
#include "otlv4.h"

namespace databus {

  class TabDef {
   public:
    std::string owner;
    std::string name;
    std::set<Ushort> pk;
    std::map<Ushort, std::string> col_names;
    std::map<Ushort, std::string> col_types;  // shall I use number?
   public:
    void dump();
  };

  typedef std::shared_ptr<TabDef> TabDefPtr;
  class MetadataManager {
    // the fucntion of MetadataManager should be like this:
    // 1. init all the tableDef from a configure file
    // 2. add a new table to tracking
    // in both cases, we know owner.table at begining
    // 3. Get Tabledef from obj#
    // 4. We should init TableDef when needed, so modify getTabDefFromId
    // TODO:
    //  a. init TableDef when get a objid effectively
    //  b. since db connection is not thread-safe, so this function is not
    //  thread-safe.
    //     need to verify only 1 thread use db connection,  all the others are
    //     read from static member only,
    //  c. With the mode stated in b,  this read/write mode doesn't need a
    //  mutex?
    //     A: on a given time, getTabDefFromId to get a TabDef, the object_id
    //     must be inited or not before, so when read this id, there is no
    //     changes
    //     for this id during the time. so doesn't need a mutex in this case?
    //

   public:
    MetadataManager(const std::string& conn_str);
    ~MetadataManager();
    // re-connect if needed
    std::shared_ptr<TabDef> getTabDefFromId(uint32_t object_id);
    std::shared_ptr<TabDef> initTabDefFromName(const char* owner,
                                               const char* table);

    std::string getLogfile(uint32_t seq);

    // return -1 if the given seq is not the current logfile
    uint32_t getOnlineLastBlock(uint32_t seq);

   private:
    void init(const std::string& username, const std::string& password,
              const std::string& db);
    uint32_t getGlobalObjId(uint32_t obj);
    void initFromId(uint32_t object_id);

   public:
    static bool haveDef(uint32_t object_id) {
      return oid2def_.find(object_id) != oid2def_.end();
    }

   private:
    // for re-connect
    const std::string conn_str_;
    otl_connect conn_;
    otl_stream tab2oid_stmt_;
    otl_stream tab2def_stmt_;
    otl_stream objp2g_stmt_;
    otl_stream obj2tab_stmt_;
    otl_stream pk_stmt_;
    static std::map<uint32_t, std::shared_ptr<TabDef> > oid2def_;
    static std::map<uint32_t, uint32_t> poid2goid_;
  };

  class LogManager {
   public:
    LogManager(const char* conn_str);
    ~LogManager();
    std::string getLogfile(uint32_t seq);
    uint32_t getOnlineLastBlock(uint32_t seq);

   private:
    // for re-connect
    const std::string conn_str_;
    otl_connect conn_;
    otl_stream arch_log_stmt_;
    otl_stream online_log_stmt_;
    otl_stream log_last_blk_stmt_;
  };
}
#endif
