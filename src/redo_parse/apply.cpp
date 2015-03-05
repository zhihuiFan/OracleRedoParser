#include "apply.h"

Applier::Applier() {}

void Applier::applyOp(std::shared_ptr<RowChange> sp) {
  auto table_ptr = table_ops_[sp->object_id_];
  if (table_ptr == NULL) {
    table_ptr = std::shared_ptr<TableOp>(new TableOp(sp->object_id_));
    table_ops_[sp->object_id_] = table_ptr;
  }
  table_ptr->applyOp(sp);
}

Applier::TableOp::TableOp(Applier* applier, uint32_t oid)
    : object_id_(oid), applier_(applier) {
  tab_def_ = getMetaData().getTabDefFromId(object_id);
}

void Applier::TableOp::applyOp(std::shared_ptr<RowChange> sp) {
  std::shared_ptr<Statement> stmt_ptr;
  if (sp->isInsert()) {
    auto key = sp->new_data_.size();
    stmt_ptr = cached_insert_stmts_[key];
    if (stmt_ptr == NULL) {
      // init stmt here
      // stmt_ptr =  xxx
    }
  } else if (sp->isUpdate()) {
    auto key = getColKey4Update(sp);
    stmt_ptr = cached_insert_stmts_[key];
    if (stmt_ptr == NULL) {
      // init stmt here
      // stmt_ptr =  xxx
    }
  } else if (sp->isDelete()) {
    if (delete_stmt_ == NULL) {
      // init it
    }
  }
  return runStatement(stmt_ptr, sp->new_data_, sp->pk_);
}

Applier::TableOp::resetAllCaches() {
  cached_insert_stmts_.clear();
  cached_update_stmts_.clear();
  delete_stmt_ = NULL;
  tab_def_ = getMetadata().getTableDefFromId(object_id_);
}

Key Applier::TableOp::getColKey4Update(std::shared_ptr<RowChange> sp) {
  Key key;
  for (auto i : sp->new_data_) {
    key[i.col_id_] = 1;
  }
  return std::move(key);
}

void applier::TableOp::runStatement(std::shared_ptr<Statement> stmt_ptr,
                                    const Row& new_data) {
  for (auto c : new_data) {
    std::string type = tab_def_->col_types[c->col_id_];
    if (type == "NUMBER") {
      stmt_ptr.setNumber(c->col_id_, asNumber(c->col_data_, c->col_len_));
    } else if (type == "VARCHAR2" || type == "CHAR") {
      stmt_ptr->setString(c->col_id_, c->col_data_);
    } else if (type == "Date") {
      stmt_ptr->setDate(c->col_id_, asDate(c->col_data_, c->col_len_));
    }
  }
  return stmt_ptr->executeUpdate();
}
