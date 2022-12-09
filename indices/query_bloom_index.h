#ifndef INDICES_QUERY_BLOOM_INDEX_H_
#define INDICES_QUERY_BLOOM_INDEX_H_

#include <stdlib.h>
#include <time.h>
#include <vector>
#include <string>
#include <tuple>
#include <utility>
#include "make_sequence_bloom_tree/id_index.h"

StatusOr<objname_to_matching_trace_ids> query_bloom_index_for_value(
    gcs::Client* client, std::string queried_value, std::string index_bucket, time_t start_time,
    time_t end_time);
StatusOr<std::vector<std::string>> is_trace_id_in_leaf(
    gcs::Client* client, std::string traceID, time_t start_time, time_t end_time, std::string index_bucket);
StatusOr<bool> is_trace_id_in_nonterminal_node(
    gcs::Client* client, std::string traceID, time_t start_time,
    time_t end_time, std::string index_bucket
);
std::vector<std::tuple<time_t, time_t>> get_children(std::tuple<time_t, time_t> parent, time_t granularity);
std::tuple<time_t, time_t> get_nearest_node(std::tuple<time_t, time_t> root, time_t granularity,
    time_t start_time, time_t end_time);

StatusOr<objname_to_matching_trace_ids> get_return_value_from_objnames(gcs::Client* client,
    std::vector<std::string> object_names,
    std::string index_bucket, std::string queried_value);
#endif  // INDICES_QUERY_BLOOM_INDEX_H
