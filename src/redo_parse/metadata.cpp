#include "metadata_otl.h"
#include "otlv4.h"

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

  MetadataManager::MetadataManager(const std::string& conn_str)
      : conn_str_(conn_str),
        tab2oid_stmt_(10,
                      "select object_id from dba_objects where "
                      "owner=upper(:x<char[31]>) and "
                      "object_name=upper(:y<char[129]>) and object_type in "
                      "('TABLE', 'TABLE PARTITION')",
                      conn_),
        tab2def_stmt_(
            10,
            "select COLUMN_ID, COLUMN_NAME, DATA_TYPE from dba_tab_cols where "
            "owner=upper(:x<char[31]>) and table_name=upper(:y<char[129]) "
            "and column_id is not null",
            conn_),
        pk_stmt_(
            10,
            "select column_id "
            " from dba_cons_columns col, dba_constraints con, dba_tab_cols tc "
            " where con.owner=col.owner "
            " and  con.table_name = col.table_name "
            " and  con.CONSTRAINT_NAME= col.CONSTRAINT_NAME "
            " and  con.CONSTRAINT_TYPE='P' "
            " and  con.owner=upper(:x<char[31]>) "
            " and  con.table_name=upper(:y<char[129]>) "
            " and  col.column_name = tc.column_name "
            " and  tc.table_name = con.table_name"
            " and  tc.owner = con.owner ",
            conn_),
        objp2g_stmt_(10,
                     " select object_id from dba_objects"
                     " where subobject_name is null and"
                     " (object_name, owner) in (select "
                     " object_name, owner from dba_objects "
                     " where object_id = :x<unsigned>)",
                     conn_),
        obj2tab_stmt_(10,
                      " select owner, object_name from dba_objects"
                      " where object_id = :x<unsigned>") {
    // make sure we called otl_connect::otl_initialize() before init this
    // class
    conn_.rlogin(conn_str);
  }

  MetadataManager::~MetadataManager() { conn_.logoff(); }

  uint32_t MetadataManager::getGlobalObjId(uint32_t objid) {
    objp2g_stmt_ << objid;
    uint32_t gid = -1;
    if (!objp2g_stmt_.eof()) {
      gid << objp2g_stmt_;
    }
    return gid;
  }

  void MetadataManager::initFromId(uint32_t object_id) {
    obj2tab_stmt_ << object_id;
    if (!obj2tab_stmt_.eof()) {
      char[31] owner;
      char[129] table;
      obj2tab_stmt_ >> owner;
      obj2tab_stmt_ >> table;
      initTabDefFromName(owner, table);
    }
  }

  std::shared_ptr<TabDef> MetadataManager::getTabDefFromId(uint32_t object_id) {
    // may be null
    // a). not init   b). object_id is a partition obj id
    if (haveDef(object_id)) return oid2def_[object_id];
    uint32_t goid = poid2goid_[object_id];
    if (goid) {
      if (haveDef(goid)) return oid2def_[goid];
    } else {
      goid = getGlobalObjId(object_id);
    }
    if (goid != object_id) poid2goid_[object_id] = goid;  // cache the map
    initFromId(goid);
    // NULL if a) not pk, like some sys.tables
    return oid2def_[goid];
  }

  std::shared_ptr<TabDef> MetadataManager::initTabDefFromName(
      const char* owner, const char* table) {

    std::shared_ptr<TabDef> tab_def(new TabDef());
    tab_def->owner = std::string(owner);
    tab_def->name = std::string(table);

    pk_stmt_ << owner << table;
    Ushort colno;
    while (!pk_stmt_.eof()) {
      pk_stmt_ >> colno;
      tab_def->pk.insert(colno);
    }
    if (tab_def->pk.empty()) {
      debug() << "either " << owner << "." << table
              << " not exits or not primary key" << std::endl;
      return NULL;
    }
    assert(!tab_def->pk.empty());

    tab2def_stmt_ << owner << table;
    Ushort col_id;
    char[129] col_name, col_type;
    while (!tab2def_stmt_->eof()) {
      tab2def_stmt_ >> col_id >> col_name >> col_type;
      tab_def->col_names[col_id] = std::move(string(col_name));
      tab_def->col_types[col_id] = std::move(string(col_type));
    }

    tab2oid_stmt_ << owner << table;
    uint32_t int object_id;
    while (oid_ret->next()) {
      tab2oid_stmt_ >> object_id;
      if (oid2def_.find(object_id) != oid2def_.end()) {
        warn() << "logical error old def exist already"
               << oid2def_[object_id]->name << ":" << oid2def_[object_id]->owner
               << " new def " << tab_def->owner << ":" << tab_def->name
               << std::endl;
      }
      oid2def_[object_id] = tab_def;
    }
    return oid2def_[object_id];
  }

  LogManager::LogManager(const char* conn_str)
      : conn_str_(conn_str),
        arch_log_stmt_(
            1,
            "select name from v$archived_log where sequence# = :seq<unsigned>",
            conn_),
        online_log_stmt_(1,
                         "select member from v$logfile lf, v$log l where "
                         "l.group# = lf.group# and l.sequence# = "
                         ":seq<unsigned>",
                         conn_),
        log_last_blk_stmt_(1,
                           "select LAST_REDO_BLOCK from v$thread where "
                           "LAST_REDO_SEQUENCE# = :seq<unsigned>") {
    conn_.rlogon(conn_str);
  }

  std::string LogManager::getLogfile(uint32_t seq) {
    // prefer archive log
    arch_log_stmt_ << seq;
    char[514] filename;
    if (!arch_log_stmt_.eof()) {
      arch_log_stmt_ >> filename;
      return std::string(filename);
    }
    online_log_stmt_ << seq;
    if (!online_log_stmt_.eof()) {
      online_log_stmt_ >> filename;
      return std::string(filename);
    }
    return std::string();
  }

  uint32_t LogManager::getOnlineLastBlock(uint32_t seq) {
    log_last_blk_stmt_ << seq;
    if (!log_last_blk_stmt_.eof()) {
      uint32_t last_blk;
      log_last_blk_stmt_ >> last_blk;
      return last_blk;
    }
    return 0;
  }

  LogManager::~LogManager() { conn_.logoff(); }
}
