// Everything related to creating the trace ID index
#ifndef  INDICES_MAKE_SEQUENCE_BLOOM_TREE_ID_INDEX_H_
#define  INDICES_MAKE_SEQUENCE_BLOOM_TREE_ID_INDEX_H_

#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <regex>
#include <vector>
#include <string>
#include <utility>
#include <tuple>
#include <map>
#include <boost/regex.hpp>
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "bloom_filter.hpp"
#include <boost/algorithm/string/regex.hpp>
#include "query_conditions.h"

// TODO(jessica): these are already defined in common under different names
const char SPAN_ID[] = "span.id";
const char SPAN_ID_BUCKET[] = "span-id";
const char TRACE_ID[] = "trace.id";
const char TRACE_ID_BUCKET[] = "trace-id";
const int branching_factor = 10;

// Leaf struct
struct Leaf {
    time_t start_time;
    time_t end_time;
    std::vector<std::string> batch_names;
    std::vector<bloom_filter> bloom_filters;
};

void serialize_leaf(Leaf leaf, std::ostream& os);
Leaf deserialize_leaf(std::istream& is);
bool leaf_sizes_equal(struct Leaf &leaf1, struct Leaf &leaf2);
bool batch_names_equal(struct Leaf &leaf1, struct Leaf &leaf2);
bool bloom_filters_equal(struct Leaf &leaf1, struct Leaf &leaf2);
bool leaf_equals(struct Leaf &leaf1, struct Leaf &leaf2);

struct BatchObjectNames {
    std::vector<std::string> inclusive;
    std::vector<std::string> early;
    std::vector<std::string> late;
};
std::vector<struct BatchObjectNames> split_batches_by_leaf(
    std::vector<std::string> object_names, time_t last_updated, time_t to_update, time_t granularity);

// Core code
std::vector<std::string> generate_prefixes(time_t earliest, time_t latest);
std::vector<std::string> get_batches_between_timestamps(gcs::Client* client, time_t earliest, time_t latest);
bloom_filter create_bloom_filter_partial_batch(gcs::Client* client, std::string batch, time_t earliest, time_t latest,
    std::string property_name, property_type prop_type, get_value_func val_func);
bloom_filter create_bloom_filter_entire_batch(gcs::Client* client, std::string batch,
    std::string property_name, property_type prop_type, get_value_func val_func);
Leaf make_leaf(gcs::Client* client, BatchObjectNames &batch, time_t start_time,
    time_t end_time, std::string index_bucket,
    std::string property_name, property_type prop_type, get_value_func val_func);
int bubble_up_leaf(gcs::Client* client, time_t start_time, time_t end_time, Leaf &leaf, std::string index_bucket);
std::tuple<time_t, time_t> get_parent(time_t start_time, time_t end_time, time_t granularity);
time_t create_index_bucket(gcs::Client* client, std::string index_bucket);
int bubble_up_bloom_filter(gcs::Client* client, bloom_filter bf, std::string index_bucket);
int update_index(gcs::Client* client, std::string property_name, time_t granularity,
    property_type prop_type, get_value_func val_func);
Status get_root_and_granularity(gcs::Client* client, std::tuple<time_t, time_t> &root,
    time_t &granularity, std::string ib);

#endif  // INDICES_MAKE_SEQUENCE_BLOOM_TREE_ID_INDEX_H_
