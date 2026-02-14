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

#include "sql/stmt/create_materialized_view_stmt.h"

#include "common/log/log.h"
#include "sql/expr/expression.h"

CreateMaterializedViewStmt::~CreateMaterializedViewStmt() = default;

RC CreateMaterializedViewStmt::create(Db *db, CreateMaterializedViewSqlNode &create_mv, Stmt *&stmt)
{
  (void)db;
  stmt = new CreateMaterializedViewStmt(create_mv.relation_name, std::move(create_mv.selection));
  LOG_DEBUG("create materialized view statement: view name %s", create_mv.relation_name.c_str());
  return RC::SUCCESS;
}
