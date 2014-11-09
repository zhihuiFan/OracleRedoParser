#ifndef METADATA_INC
#define METADATA_INC
#include <string>
#include <stdlib.h>
#include <occi.h>
#include <map>
#include <list>
#include <set>
#include <vector>
#include <assert.h>
#include <sstream>
#include <iostream>
#include "util/dtypes.h"

namespace databus {
  using namespace oracle::occi;

  class TabDef {
   public:
    std::string name;
    std::set<Ushort> pk;
    std::map<Ushort, std::string> col_names;
    std::map<Ushort, std::string> col_types;  // shall I use number?
   public:
    void dump();
  };

  class MetadataManager {
    // the fucntion of MetadataManager should be like this:
    // 1. init all the tableDef from a configure file
    // 2. add a new table to tracking
    // in both cases, we know owner.table at begining
    // 3. Get Tabledef from obj#
    // 4. We should not init TableDef when needed, so modify getTabDefFromId
    // TODO:
    //  a. init TableDef when get a objid effectively
   public:
    MetadataManager(const std::string& username, const std::string& password,
                    const std::string& db);
    ~MetadataManager();
    // re-connect if needed
    TabDef* getTabDefFromId(uint32_t object_id);
    TabDef* initTabDefFromName(const std::string& owner,
                               const std::string& table);
    // TODO: remove this method, instead with some Filter function object
    bool deserveCapture(uint32_t object_id) const {
      return oid2def_.find(object_id) != oid2def_.end();
    };

    Environment* getEnv() { return env_; }

   private:
    void init(const std::string& username, const std::string& password,
              const std::string& db);
    uint32_t getGlobalObjId(uint32_t obj);
    bool haveDef(uint32_t object_id) {
      return oid2def_.find(object_id) != oid2def_.end();
    }
    void initFromId(uint32_t object_id);

   private:
    // for re-connect
    const std::string username;
    const std::string password;
    const std::string db;
    Environment* env_;
    Connection* conn_;
    Statement* tab2oid_stmt_;
    Statement* tab2def_stmt_;
    Statement* objp2g_stmt_;
    Statement* obj2tab_stmt_;
    Statement* pk_stmt_;
    std::map<uint32_t, TabDef*> oid2def_;
    std::map<uint32_t, uint32_t> poid2goid;
  };

  extern MetadataManager* metadata;
}
#endif
