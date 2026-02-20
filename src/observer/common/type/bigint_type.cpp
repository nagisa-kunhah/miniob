#include "common/type/bigint_type.h"
#include "common/value.h"
#include "common/lang/comparator.h"
#include "common/log/log.h"

int BigIntType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::BIGINT, "left type is not bigint");
  ASSERT(right.attr_type() == AttrType::BIGINT || right.attr_type() == AttrType::INTS || right.attr_type() == AttrType::FLOATS,
      "right type is not numeric");

  if (right.attr_type() == AttrType::FLOATS) {
    float   right_val = right.get_float();
    int64_t left_val  = left.get_bigint();
    double  left_d    = static_cast<double>(left_val);
    double  right_d   = static_cast<double>(right_val);
    if (left_d < right_d) {
      return -1;
    } else if (left_d > right_d) {
      return 1;
    }
    return 0;
  }

  int64_t left_val  = left.get_bigint();
  int64_t right_val = right.get_bigint();
  return common::compare_int64((void *)&left_val, (void *)&right_val);
}

RC BigIntType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::BIGINT: {
      result.set_bigint(val.get_bigint());
      return RC::SUCCESS;
    }
    case AttrType::INTS: {
      result.set_int(static_cast<int>(val.get_bigint()));
      return RC::SUCCESS;
    }
    case AttrType::FLOATS: {
      float float_value = static_cast<float>(val.get_bigint());
      result.set_float(float_value);
      return RC::SUCCESS;
    }
    default: LOG_WARN("unsupported type %d", type); return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
}

RC BigIntType::add(const Value &left, const Value &right, Value &result) const
{
  result.set_bigint(left.get_bigint() + right.get_bigint());
  return RC::SUCCESS;
}

RC BigIntType::subtract(const Value &left, const Value &right, Value &result) const
{
  result.set_bigint(left.get_bigint() - right.get_bigint());
  return RC::SUCCESS;
}

RC BigIntType::multiply(const Value &left, const Value &right, Value &result) const
{
  result.set_bigint(left.get_bigint() * right.get_bigint());
  return RC::SUCCESS;
}

RC BigIntType::negative(const Value &val, Value &result) const
{
  result.set_bigint(-val.get_bigint());
  return RC::SUCCESS;
}

RC BigIntType::set_value_from_str(Value &val, const string &data) const
{
  RC           rc = RC::SUCCESS;
  stringstream deserialize_stream;
  deserialize_stream.clear();  // 清理stream的状态，防止多次解析出现异常
  deserialize_stream.str(data);
  int64_t bigint_value;
  deserialize_stream >> bigint_value;
  if (!deserialize_stream || !deserialize_stream.eof()) {
    rc = RC::SCHEMA_FIELD_TYPE_MISMATCH;
  } else {
    val.set_bigint(bigint_value);
  }
  return rc;
}

RC BigIntType::to_string(const Value &val, string &result) const
{
  stringstream ss;

  ss << val.value_.bigint_value_;
  result = ss.str();
  return RC::SUCCESS;
}
