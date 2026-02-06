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
// Created by AI Assistant on 2024/02/06.
//

#include "common/log/log.h"
#include "sql/operator/order_by_logical_operator.h"
#include "sql/expr/expression.h"

using namespace std;

OrderByLogicalOperator::OrderByLogicalOperator(vector<OrderByNode> &&order_by_exprs)
{
  order_by_expressions_ = std::move(order_by_exprs);
}
