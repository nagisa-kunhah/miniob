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

#pragma once

#include "sql/operator/physical_operator.h"

/**
 * @brief LIMIT 物理算子
 * @ingroup PhysicalOperator
 * @details 限制查询结果的行数
 */
class LimitPhysicalOperator : public PhysicalOperator
{
public:
  LimitPhysicalOperator(int limit);
  virtual ~LimitPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::LIMIT; }
  OpType               get_op_type() const override { return OpType::LIMIT; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;
  RC     tuple_schema(TupleSchema &schema) const override;

private:
  int limit_;      ///< 限制的行数，-1 表示没有限制
  int returned_;   ///< 已经返回的行数
};
