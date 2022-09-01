#ifndef  QUERY_BLOOM_INDEX_H_ // NOLINT
#define QUERY_BLOOM_INDEX_H_ // NOLINT

#include <stdlib.h>
#include <time.h>
#include <vector>
#include <string>
#include <tuple>
#include <utility>
#include "id_index/id_index.h"

objname_to_matching_trace_ids query_bloom_index_for_value(
    gcs::Client* client, std::string queried_value, std::string index_bucket, time_t start_time,
    time_t end_time);
std::vector<std::string> is_trace_id_in_leaf(
    gcs::Client* client, std::string traceID, time_t start_time, time_t end_time, std::string index_bucket);
bool is_trace_id_in_nonterminal_node(
    gcs::Client* client, std::string traceID, time_t start_time,
    time_t end_time, std::string index_bucket
);
std::vector<std::tuple<time_t, time_t>> get_children(std::tuple<time_t, time_t> parent, time_t granularity);
std::pair<time_t, time_t> get_nearest_node(std::pair<time_t, time_t> root, time_t granularity,
    time_t start_time, time_t end_time);

#endif // QUERY_BLOOM_INDEX_H // NOLINT
