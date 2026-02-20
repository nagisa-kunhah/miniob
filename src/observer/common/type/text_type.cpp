/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/text_type.h"
#include "common/value.h"

int TextType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::TEXT && right.attr_type() == AttrType::TEXT, "invalid type");
  return common::compare_string((void *)left.data(), left.length(), (void *)right.data(), right.length());
}

RC TextType::set_value_from_str(Value &val, const string &data) const
{
  val.set_text(data.c_str(), static_cast<int>(data.size()));
  return RC::SUCCESS;
}

RC TextType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::TEXT: {
      result.set_text(val.data(), val.length());
      return RC::SUCCESS;
    }
    case AttrType::CHARS: {
      result.set_string(val.data(), val.length());
      return RC::SUCCESS;
    }
    default: return RC::UNIMPLEMENTED;
  }
}

int TextType::cast_cost(AttrType type)
{
  if (type == AttrType::TEXT) {
    return 0;
  }
  if (type == AttrType::CHARS) {
    return 2;
  }

  return INT32_MAX;
}

RC TextType::to_string(const Value &val, string &result) const
{
  result.assign(val.data() == nullptr ? "" : val.data(), val.length());
  return RC::SUCCESS;
}
