/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "sql/operator/physical_operator.h"

/**
 * @brief LIMIT 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class LimitVecPhysicalOperator : public PhysicalOperator
{
public:
  explicit LimitVecPhysicalOperator(int limit) : limit_(limit) {}
  virtual ~LimitVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::LIMIT; }
  OpType               get_op_type() const override { return OpType::LIMIT; }

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override;
  RC close() override;

  RC tuple_schema(TupleSchema &schema) const override;

private:
  void init_output_chunk_if_need(Chunk &child_chunk);

private:
  int   limit_         = -1;
  int   returned_rows_ = 0;
  Chunk child_chunk_;
  Chunk output_chunk_;
};
