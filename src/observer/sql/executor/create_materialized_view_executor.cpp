/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by OpenAI Codex
//

#include "sql/executor/create_materialized_view_executor.h"

#include <cctype>

#include "common/lang/span.h"
#include "common/lang/string.h"
#include "common/lang/unordered_map.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/executor/sql_result.h"
#include "sql/operator/physical_operator.h"
#include "sql/optimizer/optimize_stage.h"
#include "sql/stmt/create_materialized_view_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "storage/db/db.h"
#include "storage/record/record.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

static string sanitize_column_name(const char *raw, int index)
{
  string name;
  if (raw != nullptr) {
    name = raw;
  }

  string sanitized;
  sanitized.reserve(name.size());
  for (char c : name) {
    if (std::isalnum(static_cast<unsigned char>(c)) || c == '_') {
      sanitized.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    } else {
      sanitized.push_back('_');
    }
  }

  if (sanitized.empty()) {
    sanitized = "c" + std::to_string(index);
  }
  if (std::isdigit(static_cast<unsigned char>(sanitized[0]))) {
    sanitized = "c_" + sanitized;
  }
  return sanitized;
}

static string normalize_display_name(const char *raw, int index)
{
  if (raw != nullptr && !common::is_blank(raw)) {
    return string(raw);
  }
  return "c" + std::to_string(index);
}

static int default_length_for_type(AttrType type)
{
  switch (type) {
    case AttrType::INTS: return sizeof(int32_t);
    case AttrType::FLOATS: return sizeof(float);
    case AttrType::BOOLEANS: return sizeof(int32_t);
    case AttrType::DATE: return sizeof(uint32_t);
    case AttrType::BIGINT: return sizeof(int64_t);
    case AttrType::TEXT: return 4096;
    case AttrType::CHARS: return 4;
    default: return 4;
  }
}

static RC insert_one_row(Table *table, Trx *trx, vector<Value> &values)
{
  Record record;
  RC     rc = table->make_record(static_cast<int>(values.size()), values.data(), record);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to make record. rc=%s", strrc(rc));
    return rc;
  }

  rc = trx->insert_record(table, record);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to insert record. rc=%s", strrc(rc));
  }
  return rc;
}

RC CreateMaterializedViewExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::CREATE_MATERIALIZED_VIEW,
      "create materialized view executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  auto *create_mv_stmt = static_cast<CreateMaterializedViewStmt *>(stmt);
  Db   *db             = session->get_current_db();
  if (db == nullptr) {
    return RC::SCHEMA_DB_NOT_EXIST;
  }

  const string &view_name = create_mv_stmt->view_name();

  // 1) Bind & validate select stmt
  Stmt *select_stmt_raw = nullptr;
  RC    rc              = SelectStmt::create(db, create_mv_stmt->select_sql(), select_stmt_raw);
  if (OB_FAIL(rc)) {
    return rc;
  }
  unique_ptr<SelectStmt> select_stmt(static_cast<SelectStmt *>(select_stmt_raw));

  // 2) Infer materialized view schema from select expressions
  const auto &query_expressions = select_stmt->query_expressions();
  if (query_expressions.empty()) {
    return RC::INVALID_ARGUMENT;
  }

  vector<AttrInfoSqlNode> attr_infos;
  attr_infos.reserve(query_expressions.size());
  unordered_map<string, int> used_names;
  vector<string>             display_names;
  display_names.reserve(query_expressions.size());

  for (size_t i = 0; i < query_expressions.size(); i++) {
    Expression *expr = query_expressions[i].get();
    AttrType    type = expr->value_type();
    int         len  = expr->value_length();
    if (len <= 0) {
      len = default_length_for_type(type);
    }

    display_names.emplace_back(normalize_display_name(expr->name(), static_cast<int>(i + 1)));

    string col_name = sanitize_column_name(expr->name(), static_cast<int>(i + 1));
    auto   iter     = used_names.find(col_name);
    if (iter == used_names.end()) {
      used_names.emplace(col_name, 1);
    } else {
      int suffix = ++iter->second;
      col_name += "_" + std::to_string(suffix);
    }

    AttrInfoSqlNode attr;
    attr.type   = type;
    attr.name   = col_name;
    attr.length = static_cast<size_t>(len);
    attr_infos.emplace_back(std::move(attr));
  }

  // 3) Create table for MV
  vector<string> empty_primary_keys;
  rc = db->create_table(view_name.c_str(),
      span<const AttrInfoSqlNode>(attr_infos.data(), attr_infos.size()),
      empty_primary_keys,
      StorageFormat::ROW_FORMAT);
  if (OB_FAIL(rc)) {
    return rc;
  }

  Table *mv_table = db->find_table(view_name.c_str());
  if (mv_table == nullptr) {
    return RC::INTERNAL;
  }

  rc = mv_table->set_field_display_names(span<const string>(display_names.data(), display_names.size()));
  if (OB_FAIL(rc)) {
    return rc;
  }

  // 4) Build physical plan for select and insert results into MV table
  SQLStageEvent tmp_event(sql_event->session_event(), "");
  tmp_event.set_stmt(select_stmt.release());  // ownership moved to tmp_event

  OptimizeStage optimize_stage;
  rc = optimize_stage.handle_request(&tmp_event);
  if (OB_FAIL(rc)) {
    return rc;
  }

  unique_ptr<PhysicalOperator> &oper = tmp_event.physical_operator();
  if (oper == nullptr) {
    return RC::INTERNAL;
  }

  Trx *trx = session->current_trx();
  trx->start_if_need();

  rc = oper->open(trx);
  if (OB_FAIL(rc)) {
    return rc;
  }

  const int expected_columns = static_cast<int>(attr_infos.size());
  if (session->used_chunk_mode()) {
    Chunk chunk;
    while (true) {
      rc = oper->next(chunk);
      if (rc == RC::RECORD_EOF) {
        rc = RC::SUCCESS;
        break;
      }
      if (OB_FAIL(rc)) {
        break;
      }

      if (chunk.column_num() != expected_columns) {
        rc = RC::INTERNAL;
        break;
      }

      const int     rows = chunk.rows();
      vector<Value> values;
      values.resize(expected_columns);
      for (int r = 0; r < rows && OB_SUCC(rc); r++) {
        for (int c = 0; c < expected_columns; c++) {
          values[c] = chunk.get_value(c, r);
        }
        rc = insert_one_row(mv_table, trx, values);
      }
      if (OB_FAIL(rc)) {
        break;
      }
    }
  } else {
    while (true) {
      rc = oper->next();
      if (rc == RC::RECORD_EOF) {
        rc = RC::SUCCESS;
        break;
      }
      if (OB_FAIL(rc)) {
        break;
      }

      Tuple *tuple = oper->current_tuple();
      if (tuple == nullptr) {
        rc = RC::INTERNAL;
        break;
      }
      if (tuple->cell_num() != expected_columns) {
        rc = RC::INTERNAL;
        break;
      }

      vector<Value> values;
      values.resize(expected_columns);
      for (int i = 0; i < expected_columns; i++) {
        rc = tuple->cell_at(i, values[i]);
        if (OB_FAIL(rc)) {
          break;
        }
      }
      if (OB_FAIL(rc)) {
        break;
      }

      rc = insert_one_row(mv_table, trx, values);
      if (OB_FAIL(rc)) {
        break;
      }
    }
  }

  RC close_rc = oper->close();
  if (close_rc != RC::SUCCESS) {
    LOG_WARN("failed to close select operator. rc=%s", strrc(close_rc));
    if (rc == RC::SUCCESS) {
      rc = close_rc;
    }
  }

  if (!session->is_trx_multi_operation_mode()) {
    if (rc == RC::SUCCESS) {
      rc = trx->commit();
    } else {
      RC rc2 = trx->rollback();
      if (rc2 != RC::SUCCESS) {
        LOG_PANIC("rollback failed. rc=%s", strrc(rc2));
      }
    }
    session->destroy_trx();
  }

  return rc;
}
