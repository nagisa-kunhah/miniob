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

#include "sql/operator/limit_physical_operator.h"
#include "common/log/log.h"

using namespace std;

LimitPhysicalOperator::LimitPhysicalOperator(int limit) : limit_(limit), returned_(0) {}

RC LimitPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }

  PhysicalOperator *child = children_[0].get();
  RC                rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC LimitPhysicalOperator::next()
{
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  // 如果已经返回了足够多的行，就返回 EOF
  if (limit_ >= 0 && returned_ >= limit_) {
    return RC::RECORD_EOF;
  }

  RC rc = children_[0]->next();
  if (rc == RC::SUCCESS) {
    returned_++;
  }

  return rc;
}

RC LimitPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  returned_ = 0;
  return RC::SUCCESS;
}

Tuple *LimitPhysicalOperator::current_tuple()
{
  if (children_.empty()) {
    return nullptr;
  }
  return children_[0]->current_tuple();
}

RC LimitPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  if (children_.empty()) {
    return RC::SUCCESS;
  }
  return children_[0]->tuple_schema(schema);
}
