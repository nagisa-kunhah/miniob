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
#include "sql/parser/parse_defs.h"

/**
 * @brief ORDER BY 物理算子
 * @ingroup PhysicalOperator
 * @details 对查询结果进行排序
 */
class OrderByPhysicalOperator : public PhysicalOperator
{
public:
  OrderByPhysicalOperator(vector<OrderByNode> &&order_by_exprs);
  virtual ~OrderByPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY; }
  OpType               get_op_type() const override { return OpType::ORDERBY; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;
  RC     tuple_schema(TupleSchema &schema) const override;

private:
  struct OrderByRow
  {
    unique_ptr<ValueListTuple> tuple;
    vector<Value>              order_by_values;
  };

  /// @brief 比较两个已物化行的排序键
  /// @param left_row 左侧行
  /// @param right_row 右侧行
  /// @param result 比较结果：-1表示left<right, 0表示相等, 1表示left>right
  RC compare_order_by_values(const OrderByRow &left_row, const OrderByRow &right_row, int &result) const;

private:
  vector<OrderByNode> order_by_expressions_;  ///< 排序表达式列表
  vector<unique_ptr<OrderByRow>> materialized_rows_;       ///< 物化并排序后的行
  size_t                          current_index_;          ///< 当前返回的元组索引
};
