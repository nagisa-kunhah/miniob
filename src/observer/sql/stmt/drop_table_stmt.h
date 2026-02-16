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

#pragma once

#include "common/sys/rc.h"
#include "sql/stmt/stmt.h"

class Db;

/**
 * @brief drop table语句
 * @ingroup Statement
 */
class DropTableStmt : public Stmt
{
public:
  DropTableStmt() = default;
  explicit DropTableStmt(string table_name) : table_name_(std::move(table_name)) {}

  StmtType type() const override { return StmtType::DROP_TABLE; }

  const string &table_name() const { return table_name_; }

public:
  static RC create(Db *db, const DropTableSqlNode &drop_table, Stmt *&stmt);

private:
  string table_name_;
};
