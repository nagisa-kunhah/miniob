/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to terms and conditions of Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by AI Assistant on 2024/02/07.
//

#include "sql/operator/order_by_physical_operator.h"
#include "common/log/log.h"

using namespace std;

OrderByPhysicalOperator::OrderByPhysicalOperator(vector<OrderByNode> &&order_by_exprs)
    : order_by_expressions_(std::move(order_by_exprs)), current_index_(0)
{}

RC OrderByPhysicalOperator::open(Trx *trx)
{
  materialized_rows_.clear();
  current_index_ = 0;

  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  // 1. 从子操作符中读取所有元组
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      rc = RC::INTERNAL;
      LOG_WARN("failed to get tuple from operator");
      break;
    }

    auto row   = make_unique<OrderByRow>();
    row->tuple = make_unique<ValueListTuple>();
    rc         = ValueListTuple::make(*tuple, *row->tuple);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to materialize tuple for order by. rc=%s", strrc(rc));
      break;
    }

    row->order_by_values.reserve(order_by_expressions_.size());
    for (const OrderByNode &order_by : order_by_expressions_) {
      Value value;
      rc = order_by.expr->get_value(*tuple, value);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to evaluate order by expression. rc=%s", strrc(rc));
        break;
      }
      row->order_by_values.push_back(value);
    }
    if (rc != RC::SUCCESS) {
      break;
    }

    materialized_rows_.push_back(std::move(row));
  }

  if (rc != RC::RECORD_EOF) {
    LOG_WARN("failed to get all tuples from child operator. rc=%s", strrc(rc));
    child->close();
    return rc;
  }

  // 2. 对元组进行排序
  sort(materialized_rows_.begin(),
      materialized_rows_.end(),
      [this](const unique_ptr<OrderByRow> &left_row, const unique_ptr<OrderByRow> &right_row) {
        int result = 0;
        RC  rc     = this->compare_order_by_values(*left_row, *right_row, result);
        if (rc != RC::SUCCESS) {
          LOG_WARN("failed to compare tuples. rc=%s", strrc(rc));
          return false;
        }
        return result < 0;
      });

  current_index_ = 0;
  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::next()
{
  if (current_index_ >= materialized_rows_.size()) {
    return RC::RECORD_EOF;
  }
  current_index_++;
  return RC::SUCCESS;
}

RC OrderByPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  materialized_rows_.clear();
  current_index_ = 0;
  return RC::SUCCESS;
}

Tuple *OrderByPhysicalOperator::current_tuple()
{
  if (current_index_ == 0 || current_index_ > materialized_rows_.size()) {
    return nullptr;
  }
  return materialized_rows_[current_index_ - 1]->tuple.get();
}

RC OrderByPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }
  return children_[0]->tuple_schema(schema);
}

RC OrderByPhysicalOperator::compare_order_by_values(
    const OrderByRow &left_row, const OrderByRow &right_row, int &result) const
{
  const vector<Value> &left_values  = left_row.order_by_values;
  const vector<Value> &right_values = right_row.order_by_values;
  if (left_values.size() != order_by_expressions_.size() || right_values.size() != order_by_expressions_.size()) {
    return RC::INTERNAL;
  }

  for (size_t i = 0; i < order_by_expressions_.size(); i++) {
    const OrderByNode &order_by   = order_by_expressions_[i];
    int                cmp_result = left_values[i].compare(right_values[i]);
    if (cmp_result != 0) {
      result = order_by.is_desc ? -cmp_result : cmp_result;
      return RC::SUCCESS;
    }
  }

  result = 0;
  return RC::SUCCESS;
}
