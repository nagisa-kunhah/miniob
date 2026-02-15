#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/type/date_type.h"
#include "common/value.h"
#include "common/lang/limits.h"
#include "common/value.h"
#include "storage/common/column.h"
#include <cstdio>

namespace {

inline bool is_leap_year(int year)
{
  return (year % 4 == 0) && ((year % 100 != 0) || (year % 400 == 0));
}

RC parse_date_yyyy_mm_dd(const string &data, uint32_t &packed)
{
  string s = data;
  common::strip(s);
  if (s.size() != 10 || s[4] != '-' || s[7] != '-') {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  int year  = 0;
  int month = 0;
  int day   = 0;
  try {
    year  = stoi(s.substr(0, 4));
    month = stoi(s.substr(5, 2));
    day   = stoi(s.substr(8, 2));
  } catch (...) {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  if (year < 0 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31) {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  static const int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  int             max_day          = days_in_month[month];
  if (month == 2 && is_leap_year(year)) {
    max_day = 29;
  }
  if (day > max_day) {
    return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }

  packed = static_cast<uint32_t>(year * 10000 + month * 100 + day);
  return RC::SUCCESS;
}

void format_date_yyyy_mm_dd(uint32_t packed, string &out)
{
  int year  = static_cast<int>(packed / 10000);
  int month = static_cast<int>((packed / 100) % 100);
  int day   = static_cast<int>(packed % 100);

  char buf[32];
  snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
  out.assign(buf);
}

}  // namespace

int DateType::compare(const Value &left, const Value &right) const
{
  uint32_t left_val    = left.get_date();
  uint32_t right_value = right.get_date();
  if (left_val < right_value) {
    return -1;
  }
  if (left_val > right_value) {
    return 1;
  }
  return 0;
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  uint32_t packed = 0;
  RC       rc     = parse_date_yyyy_mm_dd(data, packed);
  if (OB_FAIL(rc)) {
    return rc;
  }
  val.set_date(packed);
  return RC::SUCCESS;
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::DATE: {
      result.set_date(val.get_date());
      return RC::SUCCESS;
    }
    case AttrType::CHARS: {
      string s;
      format_date_yyyy_mm_dd(val.get_date(), s);
      result.set_string(s.c_str(), static_cast<int>(s.size()));
      return RC::SUCCESS;
    }
    default: return RC::SCHEMA_FIELD_TYPE_MISMATCH;
  }
}

RC DateType::to_string(const Value &val, string &result) const
{
  format_date_yyyy_mm_dd(val.get_date(), result);
  return RC::SUCCESS;
}
