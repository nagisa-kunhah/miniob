/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/log/log.h"
#include "sql/operator/limit_vec_physical_operator.h"

RC LimitVecPhysicalOperator::open(Trx *trx)
{
  returned_rows_ = 0;
  child_chunk_.reset();
  output_chunk_.reset();

  if (children_.empty()) {
    return RC::SUCCESS;
  }

  return children_[0]->open(trx);
}

RC LimitVecPhysicalOperator::next(Chunk &chunk)
{
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  if (limit_ >= 0 && returned_rows_ >= limit_) {
    return RC::RECORD_EOF;
  }

  RC rc = RC::SUCCESS;
  while (OB_SUCC(rc = children_[0]->next(child_chunk_))) {
    const int child_rows = child_chunk_.rows();
    if (child_rows <= 0) {
      continue;
    }

    const int left_rows = limit_ < 0 ? child_rows : (limit_ - returned_rows_);
    if (left_rows <= 0) {
      return RC::RECORD_EOF;
    }

    if (left_rows >= child_rows) {
      returned_rows_ += child_rows;
      return chunk.reference(child_chunk_);
    }

    init_output_chunk_if_need(child_chunk_);
    output_chunk_.reset_data();
    for (int col_idx = 0; col_idx < child_chunk_.column_num(); col_idx++) {
      Column &output_column = output_chunk_.column(col_idx);
      for (int row_idx = 0; row_idx < left_rows; row_idx++) {
        output_column.append_value(child_chunk_.get_value(col_idx, row_idx));
      }
    }

    returned_rows_ += left_rows;
    return chunk.reference(output_chunk_);
  }

  return rc;
}

RC LimitVecPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }

  returned_rows_ = 0;
  child_chunk_.reset();
  output_chunk_.reset();
  return RC::SUCCESS;
}

RC LimitVecPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  return children_[0]->tuple_schema(schema);
}

void LimitVecPhysicalOperator::init_output_chunk_if_need(Chunk &child_chunk)
{
  if (output_chunk_.column_num() != 0) {
    return;
  }

  for (int i = 0; i < child_chunk.column_num(); i++) {
    const Column &column = child_chunk.column(i);
    output_chunk_.add_column(make_unique<Column>(column.attr_type(), column.attr_len()), child_chunk.column_ids(i));
  }
}
