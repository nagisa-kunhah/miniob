#pragma once

#include "common/type/data_type.h"

class BigIntType : public DataType
{
public:
    BigIntType() : DataType(AttrType::BIGINT){}
    virtual ~BigIntType() {}

  int compare(const Value &left, const Value &right) const override;
  // int compare(const Column &left, const Column &right, int left_idx, int right_idx) const override;

  RC add(const Value &left, const Value &right, Value &result) const override;
  RC subtract(const Value &left, const Value &right, Value &result) const override;
  RC multiply(const Value &left, const Value &right, Value &result) const override;
  RC negative(const Value &val, Value &result) const override;

  RC cast_to(const Value &val, AttrType type, Value &result) const override;

  int cast_cost(const AttrType type) override
  {
    if (type == AttrType::BIGINT) {
      return 0;
    }
    return INT32_MAX;
  }

  RC set_value_from_str(Value &val, const string &data) const override;

  RC to_string(const Value &val, string &result) const override;
};