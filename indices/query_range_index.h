#include <stdlib.h>

#include "make_range_index/nodes.h"
#include "query_conditions.h"
#include "common.h"

StatusOr<objname_to_matching_trace_ids> query_range_index_for_value(
    gcs::Client* client, query_condition condition, std::string index_bucket,
    time_t start_time, time_t end_time);
