#include <string>
#include <iostream>
#include "metadata.h"

namespace databus {

  void TabDef::dump() {
    std::cout << "Table Name : " << name << "\n"
              << "Primary Key : "
              << "\n";
    for (auto i : pk) std::cout << i << " ";
    std::cout << std::endl;
    for (auto i : col_names)
      std::cout << col_names[i.first] << ":" << col_types[i.first] << std::endl;
  }

  // TODO: exception
  MetadataManager::MetadataManager(const std::string& user,
                                   const std::string& passwd,
                                   const std::string& db)
      : username(user), password(passwd), db(db) {
    init(user, passwd, db);
  }

  void MetadataManager::init(const std::string& user, const std::string& passwd,
                             const std::string& db) {
    env_ = Environment::createEnvironment(Environment::DEFAULT);
    conn_ = env_->createConnection(user, passwd, db);
    tab2oid_stmt_ = conn_->createStatement(
        "select object_id from dba_objects where owner=upper(:x) and "
        "object_name=upper(:y) and object_type in ('TABLE', 'TABLE "
        "PARTITION')");
    tab2def_stmt_ = conn_->createStatement(
        "select COLUMN_ID, COLUMN_NAME, DATA_TYPE from dba_tab_cols where "
        "owner=upper(:x) and table_name=upper(:y) and column_id is not null");
    pk_stmt_ = conn_->createStatement(
        "select position "
        " from dba_cons_columns col, dba_constraints con "
        " where con.owner=col.owner "
        " and  con.table_name = col.table_name "
        " and con.CONSTRAINT_NAME= col.CONSTRAINT_NAME "
        " and  con.CONSTRAINT_TYPE='P' "
        " and  con.owner=upper(:x) "
        " and  con.table_name=upper(:y) ");
    objp2g_stmt_ = conn_->createStatement(
        " select object_id from dba_objects"
        " where subobject_name is null and"
        " object_name = (select object_name "
        " from dba_objects where object_id = :x)");
    obj2tab_stmt_ = conn_->createStatement(
        " select owner, object_name from dba_objects"
        " where object_id = :x");
  }

  MetadataManager::~MetadataManager() {
    conn_->terminateStatement(tab2oid_stmt_);
    conn_->terminateStatement(pk_stmt_);
    conn_->terminateStatement(tab2def_stmt_);
    env_->terminateConnection(conn_);
    Environment::terminateEnvironment(env_);
    // let's leak memory here, doesn't matter,
    // TODO: fix this leak
    // for (auto i : oid2def_) delete i.second;
  }

  uint32_t MetadataManager::getGlobalObjId(uint32_t objid) {
    // TODO: cache the result
    objp2g_stmt_->setNumber(1, objid);
    ResultSet* ret = objp2g_stmt_->executeQuery();
    ret->next();
    return ret->getNumber(1);
  }

  void MetadataManager::initFromId(uint32_t object_id) {
    obj2tab_stmt_->setNumber(1, object_id);
    ResultSet* ret = obj2tab_stmt_->executeQuery();
    ret->next();
    std::string owner = ret->getString(1);
    std::string table = ret->getString(2);
    initTabDefFromName(owner, table);
  }

  TabDef* MetadataManager::getTabDefFromId(uint32_t object_id) {
    // may be null
    // a). not init   b). object_id is a partition obj id
    if (haveDef(object_id)) return oid2def_[object_id];
    uint32_t goid = getGlobalObjId(object_id);
    // if goid == object_id =>  non partition table
    if (haveDef(goid)) return oid2def_[goid];
    initFromId(goid);
    // NULL if a) not pk, like some sys.tables
    return oid2def_[goid];
  }

  TabDef* MetadataManager::initTabDefFromName(const std::string& owner,
                                              const std::string& table) {

    TabDef* tab_def = new TabDef();
    tab_def->name = owner + "." + table;

    pk_stmt_->setString(1, owner);
    pk_stmt_->setString(2, table);
    ResultSet* pk_ret = pk_stmt_->executeQuery();
    while (pk_ret->next()) {
      tab_def->pk.insert(pk_ret->getNumber(1).operator unsigned int());
    }
    if (tab_def->pk.empty()) {
      std::cout << "either " << owner << "." << table
                << " not exits or not primary key" << std::endl;
      return NULL;
    }
    assert(!tab_def->pk.empty());
    pk_stmt_->closeResultSet(pk_ret);

    tab2def_stmt_->setString(1, owner);
    tab2def_stmt_->setString(2, table);
    ResultSet* def_ret = tab2def_stmt_->executeQuery();
    Ushort col_id;
    while (def_ret->next()) {
      col_id = def_ret->getNumber(1).operator unsigned short();
      std::string col_name = def_ret->getString(2);
      std::string col_type = def_ret->getString(3);

      tab_def->col_names[col_id] = col_name;
      tab_def->col_types[col_id] = col_type;
    }
    tab2oid_stmt_->closeResultSet(def_ret);

    tab2oid_stmt_->setString(1, owner);
    tab2oid_stmt_->setString(2, table);
    ResultSet* oid_ret = tab2oid_stmt_->executeQuery();
    int object_id;
    while (oid_ret->next()) {
      object_id = oid_ret->getNumber(1).operator unsigned long();
      oid2def_[object_id] = tab_def;
    }
    tab2oid_stmt_->closeResultSet(oid_ret);
    return tab_def;
  }
}
