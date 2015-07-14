#include <sstream>
#include <cstring>
#include <unistd.h>
#include <boost/algorithm/string.hpp>

#define OTL_ORA11G_R2
#define OTL_ORA_UTF8
#include "otlv4.h"
#include "util/logger.h"
#include "util/dassert.h"
#include "metadata.h"

namespace databus {
  std::string TabDef::toString() {
    std::stringstream ss;
    ss << "Table Name : " << name << "\n"
       << "Primary Key : "
       << "\n";
    for (auto i : pk) ss << i << " ";
    ss << std::endl;
    for (auto i : col_names)
      ss << col_names[i.first] << ":" << col_types[i.first] << std::endl;
    return ss.str();
  }

  std::map<uint32_t, std::shared_ptr<TabDef> > MetadataManager::oid2def_;
  std::map<uint32_t, uint32_t> MetadataManager::poid2goid_;
  MetadataManager::MetadataManager(const std::string& conn_str)
      : conn_str_(conn_str),
        conn_(conn_str.c_str()),
        tab2oid_stmt_(10,
                      "select object_id, nvl(SUBOBJECT_NAME, '__') from "
                      "dba_objects where "
                      "owner=upper(:x<char[31]>) and "
                      "object_name=upper(:y<char[129]>) and object_type in "
                      "('TABLE', 'TABLE PARTITION')",
                      conn_),
        tab2def_stmt_(
            10,
            "select COLUMN_ID, COLUMN_NAME, DATA_TYPE,  DATA_LENGTH, "
            "NVL(DATA_PRECISION, 9999), NVL(DATA_SCALE, 9999) "
            "from dba_tab_cols where "
            "owner=upper(:x<char[31]>) and table_name=upper(:y<char[129]>) "
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
                      " where object_id = :x<unsigned>",
                      conn_) {
    // make sure we called otl_connect::otl_initialize() before init this
    // class
  }

  MetadataManager::~MetadataManager() { conn_.logoff(); }

  uint32_t MetadataManager::getGlobalObjId(uint32_t objid) {
    objp2g_stmt_ << objid;
    uint32_t gid = 0;
    if (!objp2g_stmt_.eof()) {
      objp2g_stmt_ >> gid;
    }
    return gid;
  }

  void MetadataManager::initFromId(uint32_t object_id) {
    obj2tab_stmt_ << object_id;
    if (!obj2tab_stmt_.eof()) {
      char owner[31];
      char table[129];
      obj2tab_stmt_ >> owner;
      obj2tab_stmt_ >> table;
      initTabDefFromName(owner, table);
    }
  }

  std::shared_ptr<TabDef> MetadataManager::getTabDefFromId(uint32_t object_id,
                                                           bool allow_init) {
    // may be null
    // a). not init   b). object_id is a partition obj id
    if (haveDef(object_id)) return oid2def_[object_id];
    uint32_t goid = getCachedGlobalId(object_id);
    if (goid == 0) {
      return NULL;
    }
    if (haveDef(goid)) return oid2def_[goid];
    if (!allow_init) {
      return NULL;
    }
    initFromId(goid);
    // NULL if a) not pk, like some sys.tables
    return oid2def_[goid];
  }

  std::shared_ptr<TabDef> MetadataManager::initTabDefFromName(
      const char* owner, const char* table) {

    std::shared_ptr<TabDef> tab_def(new TabDef());
    tab_def->owner = std::string(owner);
    tab_def->name = std::string(table);
    boost::to_upper(tab_def->owner);
    boost::to_upper(tab_def->name);

    pk_stmt_ << owner << table;
    while (!pk_stmt_.eof()) {
      unsigned int col_no;
      pk_stmt_ >> col_no;
      tab_def->pk.insert(col_no);
    }
    if (tab_def->pk.empty()) {
      LOG(ERROR) << "either " << owner << "." << table
                 << " not exists or hasn't primary key";
      return NULL;
    }
    tab2def_stmt_ << owner << table;
    unsigned int col_id;
    char col_name[129], col_type[129];
    unsigned int len;
    while (!tab2def_stmt_.eof()) {
      tab2def_stmt_ >> col_id;
      tab2def_stmt_ >> col_name;
      tab2def_stmt_ >> col_type;
      tab_def->col_names[col_id] = std::move(std::string(col_name));
      tab_def->col_types[col_id] = std::move(std::string(col_type));
      if (strcmp(col_type, "VARCHAR2") == 0 || strcmp(col_type, "CHAR") == 0) {
        tab2def_stmt_ >> len;
        tab_def->col_len[col_id] = len;
        tab2def_stmt_ >> len;
        tab2def_stmt_ >> len;
      } else if (strcmp(col_type, "NUMBER") == 0) {
        uint32_t scale;
        tab2def_stmt_ >> len;
        tab2def_stmt_ >> len;
        tab2def_stmt_ >> scale;
        tab_def->col_len[col_id] = len;
        tab_def->col_scale[col_id] = scale;
      } else {
        tab2def_stmt_ >> len;
        tab2def_stmt_ >> len;
        tab2def_stmt_ >> len;
      }
    }

    tab2oid_stmt_ << owner << table;
    uint32_t object_id;
    unsigned char object_name[129];
    uint32_t global_object_id = 0;
    while (!tab2oid_stmt_.eof()) {
      tab2oid_stmt_ >> object_id >> object_name;
      if (oid2def_.find(object_id) != oid2def_.end()) {
        LOG(WARNING) << "logical error old def exist already"
                     << oid2def_[object_id]->name << ":"
                     << oid2def_[object_id]->owner << " new def "
                     << tab_def->owner << ":" << tab_def->name << std::endl;
      }
      if (strcmp((const char*)object_name, "__") == 0) {
        // global object_id
        if (global_object_id > 0) {
          util::dassert("Assert Global_object_id Error",
                        global_object_id == object_id);
        }
        oid2def_[object_id] = tab_def;
        global_object_id = object_id;
      } else {
        // partition object_id
        if (global_object_id == 0) {
          global_object_id = getGlobalObjId(object_id);
          util::dassert("GlobalIDNotZeroError", global_object_id != 0);
        }
        poid2goid_[object_id] = global_object_id;
      }
    }
    util::dassert("Can't find out global ID ", global_object_id > 0);
    return oid2def_[global_object_id];
  }

  LogManager::LogManager(const char* conn_str)
      : conn_str_(conn_str),
        conn_(conn_str),
        arch_log_stmt_(1,
                       "select name from v$archived_log where sequence# = "
                       ":seq<unsigned> and STANDBY_DEST='NO'",
                       conn_),
        online_log_stmt_(1,
                         "select member from v$logfile lf, v$log l where "
                         "l.group# = lf.group# and l.sequence# = "
                         ":seq<unsigned>",
                         conn_),
        log_last_blk_stmt_(1,
                           "select LAST_REDO_BLOCK from v$thread where "
                           "LAST_REDO_SEQUENCE# = :seq<unsigned>",
                           conn_),
        log_file_from_scn_stmt_(1,
                                "select  SEQUENCE# from v$archived_log "
                                "where to_number(:restart_scn<char[80]>) "
                                "between FIRST_CHANGE# and NEXT_CHANGE# "
                                "and STANDBY_DEST='NO'",
                                conn_),
        online_log_seq_from_scn_stmt_(1,
                                      "select sequence# from v$log where "
                                      "to_number(:restart_scn<char[80]>) "
                                      "between FIRST_CHANGE# and NEXT_CHANGE#",
                                      conn_) {}

  uint32_t LogManager::getSeqFromScn(const char* restart_scn) {
    log_file_from_scn_stmt_ << restart_scn;
    uint32_t seq;
    if (!log_file_from_scn_stmt_.eof()) {
      log_file_from_scn_stmt_ >> seq;
      return seq;
    }
    online_log_seq_from_scn_stmt_ << restart_scn;
    if (!online_log_seq_from_scn_stmt_.eof()) {
      online_log_seq_from_scn_stmt_ >> seq;
      return seq;
    }
    return 0;
  }

  std::string LogManager::getLogfile(uint32_t seq) {
  // prefer archive log
  tryagain:
    arch_log_stmt_ << seq;
    char filename[514];
    if (!arch_log_stmt_.eof()) {
      arch_log_stmt_ >> filename;
      return std::string(filename);
    }
    online_log_stmt_ << seq;
    if (!online_log_stmt_.eof()) {
      online_log_stmt_ >> filename;
      return std::string(filename);
    }
    LOG(DEBUG) << "Seems a software bug, request seq " << seq
               << " but it is not there, sleep 3 sconds and try again";
    sleep(3);
    goto tryagain;
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
