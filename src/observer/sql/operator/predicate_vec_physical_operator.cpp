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

#include "sql/operator/predicate_vec_physical_operator.h"

#include "common/log/log.h"

PredicateVecPhysicalOperator::PredicateVecPhysicalOperator(unique_ptr<Expression> expr) : expression_(std::move(expr))
{
  ASSERT(expression_ != nullptr, "predicate vec operator should have expression");
  ASSERT(expression_->value_type() == AttrType::BOOLEANS, "predicate's expression should be BOOLEAN type");
}

RC PredicateVecPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 1) {
    LOG_WARN("predicate vec operator must has one child");
    return RC::INTERNAL;
  }
  filtered_init_ = false;
  return children_[0]->open(trx);
}

RC PredicateVecPhysicalOperator::next(Chunk &chunk)
{
  RC rc = RC::SUCCESS;
  while (OB_SUCC(rc = children_[0]->next(input_chunk_))) {
    if (!filtered_init_) {
      for (int i = 0; i < input_chunk_.column_num(); i++) {
        auto col = input_chunk_.column(i).clone();
        col->reset_data();
        filtered_chunk_.add_column(std::move(col), input_chunk_.column_ids(i));
      }
      filtered_init_ = true;
    } else {
      filtered_chunk_.reset_data();
    }

    select_.assign(input_chunk_.rows(), 1);
    rc = expression_->eval(input_chunk_, select_);
    if (OB_FAIL(rc)) {
      return rc;
    }

    for (int i = 0; i < input_chunk_.rows(); i++) {
      if (select_[i] == 0) {
        continue;
      }
      for (int j = 0; j < input_chunk_.column_num(); j++) {
        RC rc2 = filtered_chunk_.column(j).append_value(input_chunk_.column(j).get_value(i));
        if (OB_FAIL(rc2)) {
          return rc2;
        }
      }
    }

    if (filtered_chunk_.rows() > 0) {
      chunk.reference(filtered_chunk_);
      return RC::SUCCESS;
    }
    // All rows filtered out in this chunk, continue to next
  }
  return rc;
}

RC PredicateVecPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
