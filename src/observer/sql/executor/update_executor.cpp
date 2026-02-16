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
// Created by OpenAI Codex on 2026/02/16.
//

#include "sql/executor/update_executor.h"
#include <string>
#include <vector>
#include "common/log/log.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "session/session.h"
#include "sql/stmt/update_stmt.h"
#include "storage/record/lob_handler.h"
#include "storage/record/record_scanner.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;
using namespace common;

namespace {

RC build_value_from_filter_obj(const FilterObj &obj, Value &value, const Record &record)
{
  if (!obj.is_attr) {
    value = obj.value;
    return RC::SUCCESS;
  }

  const FieldMeta *field = obj.field.meta();
  value.reset();
  if (field->type() == AttrType::TEXT) {
    const Table *table = obj.field.table();
    if (table == nullptr || table->lob_handler() == nullptr) {
      LOG_WARN("lob handler is null while reading text value. table=%s", table == nullptr ? "(null)" : table->name());
      return RC::INTERNAL;
    }
    const TextLobRef &ref = *reinterpret_cast<const TextLobRef *>(record.data() + field->offset());
    if (ref.length == 0) {
      value.set_text("", 0);
      return RC::SUCCESS;
    }
    string buf;
    buf.resize(static_cast<size_t>(ref.length));
    RC rc = table->lob_handler()->get_data(ref.offset, ref.length, buf.data());
    if (OB_FAIL(rc)) {
      LOG_WARN("failed to read text from lob. offset=%ld, len=%ld, rc=%s", ref.offset, ref.length, strrc(rc));
      return rc;
    }
    value.set_text(buf.data(), static_cast<int>(ref.length));
    return RC::SUCCESS;
  }

  value.set_type(field->type());
  value.set_data(record.data() + field->offset(), field->len());
  return RC::SUCCESS;
}

RC match_filter(const FilterStmt *filter_stmt, const Record &record, bool &matched)
{
  matched = true;
  if (filter_stmt == nullptr) {
    return RC::SUCCESS;
  }

  const auto &units = filter_stmt->filter_units();
  for (const FilterUnit *unit : units) {
    Value left_value;
    Value right_value;
    RC rc = build_value_from_filter_obj(unit->left(), left_value, record);
    if (OB_FAIL(rc)) {
      return rc;
    }
    rc = build_value_from_filter_obj(unit->right(), right_value, record);
    if (OB_FAIL(rc)) {
      return rc;
    }

    int  cmp_result = left_value.compare(right_value);
    bool ok         = false;
    switch (unit->comp()) {
      case EQUAL_TO: ok = (cmp_result == 0); break;
      case LESS_EQUAL: ok = (cmp_result <= 0); break;
      case NOT_EQUAL: ok = (cmp_result != 0); break;
      case LESS_THAN: ok = (cmp_result < 0); break;
      case GREAT_EQUAL: ok = (cmp_result >= 0); break;
      case GREAT_THAN: ok = (cmp_result > 0); break;
      default: return RC::INVALID_ARGUMENT;
    }
    if (!ok) {
      matched = false;
      return RC::SUCCESS;
    }
  }

  return RC::SUCCESS;
}

RC set_value_to_record(Table *table, const FieldMeta *field, const Value &value, Record &record)
{
  if (field->type() == AttrType::TEXT) {
    if (table->lob_handler() == nullptr) {
      LOG_WARN("lob handler is null for text field. table=%s, field=%s", table->name(), field->name());
      return RC::INTERNAL;
    }
    if (value.attr_type() != AttrType::CHARS && value.attr_type() != AttrType::TEXT) {
      LOG_WARN("invalid value type for text field. table=%s, field=%s, value_type=%s",
          table->name(),
          field->name(),
          attr_type_to_string(value.attr_type()));
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }

    const int64_t text_len = static_cast<int64_t>(value.length());
    if (text_len > 65535) {
      LOG_WARN("text value too long. table=%s, field=%s, len=%ld", table->name(), field->name(), text_len);
      return RC::INVALID_ARGUMENT;
    }

    TextLobRef ref;
    ref.length = text_len;
    if (text_len > 0) {
      RC rc = table->lob_handler()->insert_data(ref.offset, ref.length, value.data());
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to insert text into lob. table=%s, field=%s, rc=%s",
            table->name(),
            field->name(),
            strrc(rc));
        return rc;
      }
    }
    memcpy(record.data() + field->offset(), &ref, sizeof(ref));
    return RC::SUCCESS;
  }

  char *dst = record.data() + field->offset();
  if (field->type() == AttrType::CHARS) {
    memset(dst, 0, field->len());
    size_t copy_len = field->len();
    const size_t data_len = value.length();
    if (copy_len > data_len) {
      copy_len = data_len + 1;
    }
    memcpy(dst, value.data(), copy_len);
    return RC::SUCCESS;
  }

  memcpy(dst, value.data(), field->len());
  return RC::SUCCESS;
}

}  // namespace

RC UpdateExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::UPDATE,
      "update executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);
  Table      *table       = update_stmt->table();
  Trx        *trx         = session->current_trx();

  RC rc = trx->start_if_need();
  if (OB_FAIL(rc)) {
    return rc;
  }

  RecordScanner *scanner = nullptr;
  rc = table->get_record_scanner(scanner, trx, ReadWriteMode::READ_WRITE);
  if (OB_FAIL(rc)) {
    return rc;
  }

  vector<Record> records;
  Record         record;
  while (OB_SUCC(rc = scanner->next(record))) {
    bool matched = false;
    rc           = match_filter(update_stmt->filter_stmt(), record, matched);
    if (OB_FAIL(rc)) {
      break;
    }
    if (!matched) {
      continue;
    }

    Record record_copy;
    rc = record_copy.copy_data(record.data(), record.len());
    if (OB_FAIL(rc)) {
      break;
    }
    record_copy.set_rid(record.rid());
    records.emplace_back(std::move(record_copy));
  }

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }

  if (scanner != nullptr) {
    RC close_rc = scanner->close_scan();
    if (close_rc != RC::SUCCESS) {
      LOG_WARN("failed to close record scanner");
    }
    delete scanner;
    scanner = nullptr;
  }

  if (OB_FAIL(rc)) {
    return rc;
  }

  const FieldMeta *field = update_stmt->field();
  const Value     &value = update_stmt->value();
  for (Record &old_record : records) {
    Record new_record;
    rc = new_record.copy_data(old_record.data(), old_record.len());
    if (OB_FAIL(rc)) {
      return rc;
    }
    new_record.set_rid(old_record.rid());

    rc = set_value_to_record(table, field, value, new_record);
    if (OB_FAIL(rc)) {
      return rc;
    }

    rc = trx->update_record(table, old_record, new_record);
    if (OB_FAIL(rc)) {
      return rc;
    }
  }

  return RC::SUCCESS;
}
