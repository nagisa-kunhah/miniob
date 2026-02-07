/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You may use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/group_by_vec_physical_operator.h"
#include "sql/operator/group_by_physical_operator.h"
#include "sql/expr/aggregate_hash_table.h"
#include "common/log/log.h"

using namespace std;
using namespace common;

GroupByVecPhysicalOperator::GroupByVecPhysicalOperator(
    vector<unique_ptr<Expression>> &&group_by_exprs, vector<Expression *> &&expressions)
    : GroupByPhysicalOperator(std::move(expressions)), group_by_exprs_(std::move(group_by_exprs))
{}

RC GroupByVecPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 1) {
    LOG_WARN("group by operator should have 1 child");
    return RC::INTERNAL;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("fail to open child operator. rc = %s", strrc(rc));
    return rc;
  }

  aggregate_hash_table_ = make_unique<StandardAggregateHashTable>(aggregate_expressions_);

  output_chunk_.reset();

  int col_id = 0;
  for (const auto &expr : group_by_exprs_) {
    output_chunk_.add_column(make_unique<Column>(expr->value_type(), expr->value_length()), col_id++);
  }

  for (Expression *expr : aggregate_expressions_) {
    auto       *aggregate_expr = static_cast<AggregateExpr *>(expr);
    Expression *child_expr     = aggregate_expr->child().get();
    output_chunk_.add_column(make_unique<Column>(child_expr->value_type(), child_expr->value_length()), col_id++);
  }

  Chunk groups_chunk;
  Chunk aggrs_chunk;

  col_id = 0;
  for (const auto &expr : group_by_exprs_) {
    groups_chunk.add_column(make_unique<Column>(expr->value_type(), expr->value_length()), col_id++);
  }

  col_id = 0;
  for (Expression *expr : aggregate_expressions_) {
    auto       *aggregate_expr = static_cast<AggregateExpr *>(expr);
    Expression *child_expr     = aggregate_expr->child().get();
    aggrs_chunk.add_column(make_unique<Column>(child_expr->value_type(), child_expr->value_length()), col_id++);
  }

  // 逐批处理子操作符返回的数据
  Chunk child_chunk;
  while (OB_SUCC(rc = child->next(child_chunk))) {
    if (child_chunk.rows() == 0) {
      continue;
    }

    // 计算 GROUP BY 表达式
    groups_chunk.reset_data();
    for (size_t i = 0; i < group_by_exprs_.size(); i++) {
      Expression *expr   = group_by_exprs_[i].get();
      RC          get_rc = expr->get_column(child_chunk, groups_chunk.column(i));
      if (OB_FAIL(get_rc)) {
        LOG_WARN("failed to get column for group by expression %d. rc=%s", i, strrc(get_rc));
        return get_rc;
      }
    }

    aggrs_chunk.reset_data();
    for (size_t i = 0; i < aggregate_expressions_.size(); i++) {
      auto       *aggregate_expr = static_cast<AggregateExpr *>(aggregate_expressions_[i]);
      Expression *child_expr     = aggregate_expr->child().get();
      RC          get_rc         = child_expr->get_column(child_chunk, aggrs_chunk.column(i));
      if (OB_FAIL(get_rc)) {
        LOG_WARN("failed to get column for aggregate expression %d. rc=%s", i, strrc(get_rc));
        return get_rc;
      }
    }

    // 将数据添加到哈希表
    RC add_rc = aggregate_hash_table_->add_chunk(groups_chunk, aggrs_chunk);
    if (OB_FAIL(add_rc)) {
      LOG_WARN("failed to add chunk to hash table. rc=%s", strrc(add_rc));
      return add_rc;
    }
  }

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }

  // 创建哈希表扫描器
  if (rc == RC::SUCCESS) {
    hash_table_scanner_ = make_unique<StandardAggregateHashTable::Scanner>(aggregate_hash_table_.get());
    hash_table_scanner_->open_scan();
  }

  return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
  if (children_.empty() || !hash_table_scanner_) {
    return RC::RECORD_EOF;
  }

  output_chunk_.reset_data();
  RC rc = hash_table_scanner_->next(output_chunk_);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to get next chunk from hash table scanner. rc=%s", strrc(rc));
    return rc;
  }

  if (rc == RC::RECORD_EOF) {
    return rc;
  }

  // 将结果引用到输出 chunk
  rc = chunk.reference(output_chunk_);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to reference output chunk. rc=%s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC GroupByVecPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }

  hash_table_scanner_.reset();
  aggregate_hash_table_.reset();

  LOG_INFO("close group by operator");
  return RC::SUCCESS;
}
