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
#include "sql/parser/parse_defs.h"

/**
 * @brief ORDER BY 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class OrderByVecPhysicalOperator : public PhysicalOperator
{
public:
  explicit OrderByVecPhysicalOperator(vector<OrderByNode> &&order_by_expressions)
      : order_by_expressions_(std::move(order_by_expressions))
  {}
  virtual ~OrderByVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY; }
  OpType               get_op_type() const override { return OpType::ORDERBY; }

  RC open(Trx *trx) override;
  RC next(Chunk &chunk) override;
  RC close() override;

  RC tuple_schema(TupleSchema &schema) const override;

private:
  struct MaterializedRow
  {
    vector<Value> row_values;
    vector<Value> order_by_values;
  };

  RC compare_row(const MaterializedRow &left_row, const MaterializedRow &right_row, int &result) const;

private:
  vector<OrderByNode>                 order_by_expressions_;
  vector<unique_ptr<MaterializedRow>> materialized_rows_;
  vector<pair<AttrType, int>>         output_column_types_;
  vector<int>                         output_column_ids_;
  size_t                              output_pos_ = 0;
  Chunk                               child_chunk_;
  Chunk                               output_chunk_;
};
