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

#pragma once

#include "common/lang/string.h"
#include "sql/stmt/stmt.h"

class Db;

/**
 * @brief 表示创建物化视图的语句
 * @ingroup Statement
 */
class CreateMaterializedViewStmt : public Stmt
{
public:
  CreateMaterializedViewStmt(string view_name, SelectSqlNode &&select_sql)
      : view_name_(std::move(view_name)), select_sql_(std::move(select_sql))
  {}
  ~CreateMaterializedViewStmt() override;

  StmtType type() const override { return StmtType::CREATE_MATERIALIZED_VIEW; }

  const string        &view_name() const { return view_name_; }
  SelectSqlNode       &select_sql() { return select_sql_; }
  const SelectSqlNode &select_sql() const { return select_sql_; }

  static RC create(Db *db, CreateMaterializedViewSqlNode &create_mv, Stmt *&stmt);

private:
  string        view_name_;
  SelectSqlNode select_sql_;
};
