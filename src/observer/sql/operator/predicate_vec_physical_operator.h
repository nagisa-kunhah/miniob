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

#include "sql/expr/expression.h"
#include "sql/operator/physical_operator.h"
#include "storage/common/chunk.h"

/**
 * @brief 向量化过滤/谓词物理算子
 * @ingroup PhysicalOperator
 */
class PredicateVecPhysicalOperator : public PhysicalOperator
{
public:
  explicit PredicateVecPhysicalOperator(unique_ptr<Expression> expr);

  virtual ~PredicateVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::PREDICATE_VEC; }
  OpType               get_op_type() const override { return OpType::FILTER; }

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override;
  RC close() override;

private:
  unique_ptr<Expression> expression_;
  Chunk                  input_chunk_;
  Chunk                  filtered_chunk_;
  bool                   filtered_init_ = false;
  vector<uint8_t>         select_;
};
