#include "common/lang/comparator.h"
#include "common/lang/sstream.h"
#include "common/log/log.h"
#include "common/type/date_type.h"
#include "common/value.h"
#include "common/lang/limits.h"
#include "common/value.h"
#include "storage/common/column.h"

int DateType::compare(const Value &left, const Value &right) const
{
    uint32_t left_val = left.get_date();
    uint32_t right_value = right.get_date();
    if (left_val<right_value) {
        return -1;
    }
    if (left_val>right_value) {
        return 1;
    }
    return 0;
}
