/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>

#include "common/log/log.h"
#include "sql/operator/order_by_vec_physical_operator.h"

RC OrderByVecPhysicalOperator::open(Trx *trx)
{
  materialized_rows_.clear();
  output_column_types_.clear();
  output_column_ids_.clear();
  output_pos_ = 0;
  child_chunk_.reset();
  output_chunk_.reset();

  if (children_.empty()) {
    return RC::SUCCESS;
  }

  RC rc = children_[0]->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  while (OB_SUCC(rc = children_[0]->next(child_chunk_))) {
    const int row_num = child_chunk_.rows();
    if (row_num <= 0) {
      continue;
    }

    if (output_column_types_.empty()) {
      for (int col_idx = 0; col_idx < child_chunk_.column_num(); col_idx++) {
        const Column &col = child_chunk_.column(col_idx);
        output_column_types_.emplace_back(col.attr_type(), col.attr_len());
        output_column_ids_.push_back(child_chunk_.column_ids(col_idx));
      }
    }

    vector<Column> order_by_columns(order_by_expressions_.size());
    for (size_t i = 0; i < order_by_expressions_.size(); i++) {
      rc = order_by_expressions_[i].expr->get_column(child_chunk_, order_by_columns[i]);
      if (OB_FAIL(rc)) {
        LOG_WARN("failed to evaluate order by expression. rc=%s", strrc(rc));
        return rc;
      }
    }

    for (int row_idx = 0; row_idx < row_num; row_idx++) {
      auto materialized_row = make_unique<MaterializedRow>();
      materialized_row->row_values.reserve(child_chunk_.column_num());
      for (int col_idx = 0; col_idx < child_chunk_.column_num(); col_idx++) {
        materialized_row->row_values.push_back(child_chunk_.get_value(col_idx, row_idx));
      }

      materialized_row->order_by_values.reserve(order_by_columns.size());
      for (Column &order_by_column : order_by_columns) {
        materialized_row->order_by_values.push_back(order_by_column.get_value(row_idx));
      }
      materialized_rows_.push_back(std::move(materialized_row));
    }
  }

  if (rc != RC::RECORD_EOF) {
    LOG_WARN("failed to consume child chunks for order by(vec). rc=%s", strrc(rc));
    return rc;
  }

  std::sort(materialized_rows_.begin(),
      materialized_rows_.end(),
      [this](const unique_ptr<MaterializedRow> &left_row, const unique_ptr<MaterializedRow> &right_row) {
        int result = 0;
        RC  rc     = compare_row(*left_row, *right_row, result);
        if (OB_FAIL(rc)) {
          LOG_WARN("failed to compare order by row. rc=%s", strrc(rc));
          return false;
        }
        return result < 0;
      });

  output_chunk_.reset();
  for (size_t i = 0; i < output_column_types_.size(); i++) {
    auto [attr_type, attr_len] = output_column_types_[i];
    output_chunk_.add_column(make_unique<Column>(attr_type, attr_len), output_column_ids_[i]);
  }

  output_pos_ = 0;
  return RC::SUCCESS;
}

RC OrderByVecPhysicalOperator::next(Chunk &chunk)
{
  if (output_pos_ >= materialized_rows_.size()) {
    return RC::RECORD_EOF;
  }

  output_chunk_.reset_data();

  const size_t output_rows = std::min(materialized_rows_.size() - output_pos_, static_cast<size_t>(Chunk::MAX_ROWS));
  for (size_t row_offset = 0; row_offset < output_rows; row_offset++) {
    const MaterializedRow &materialized_row = *materialized_rows_[output_pos_ + row_offset];
    for (int col_idx = 0; col_idx < output_chunk_.column_num(); col_idx++) {
      output_chunk_.column(col_idx).append_value(materialized_row.row_values[col_idx]);
    }
  }

  output_pos_ += output_rows;
  return chunk.reference(output_chunk_);
}

RC OrderByVecPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }

  materialized_rows_.clear();
  output_column_types_.clear();
  output_column_ids_.clear();
  output_pos_ = 0;
  child_chunk_.reset();
  output_chunk_.reset();
  return RC::SUCCESS;
}

RC OrderByVecPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  return children_[0]->tuple_schema(schema);
}

RC OrderByVecPhysicalOperator::compare_row(
    const MaterializedRow &left_row, const MaterializedRow &right_row, int &result) const
{
  if (left_row.order_by_values.size() != order_by_expressions_.size() ||
      right_row.order_by_values.size() != order_by_expressions_.size()) {
    return RC::INTERNAL;
  }

  for (size_t i = 0; i < order_by_expressions_.size(); i++) {
    int cmp_result = left_row.order_by_values[i].compare(right_row.order_by_values[i]);
    if (cmp_result != 0) {
      result = order_by_expressions_[i].is_desc ? -cmp_result : cmp_result;
      return RC::SUCCESS;
    }
  }

  result = 0;
  return RC::SUCCESS;
}
