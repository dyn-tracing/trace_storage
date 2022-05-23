// Everything related to creating the trace ID index
#ifndef  MICROSERVICES_ENV_TRACE_STORAGE_ID_INDEX_ID_INDEX_H_ // NOLINT
#define  MICROSERVICES_ENV_TRACE_STORAGE_ID_INDEX_ID_INDEX_H_ // NOLINT

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
#include "graph_query.h"

// TODO(jessica): these are already defined in common under different names
const char trace_struct_bucket[] = "dyntraces-snicket4";
const char index_bucket[] = "idindex-snicket4";
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
bloom_filter create_bloom_filter_partial_batch(gcs::Client* client, std::string batch, time_t earliest, time_t latest);
bloom_filter create_bloom_filter_entire_batch(gcs::Client* client, std::string batch);
Leaf make_leaf(gcs::Client* client, BatchObjectNames &batch, time_t start_time, time_t end_time);
int bubble_up_leaf(gcs::Client* client, time_t start_time, time_t end_time, Leaf &leaf);
std::tuple<time_t, time_t> get_parent(time_t start_time, time_t end_time, time_t granularity);
int create_index_bucket(gcs::Client* client);
std::tuple<time_t, time_t> get_parent(time_t start_time, time_t end_time, time_t granularity);
int bubble_up_bloom_filter(gcs::Client* client, bloom_filter bf);
int update_index(gcs::Client* client, time_t last_updated);



#endif  // MICROSERVICES_ENV_TRACE_STORAGE_ID_INDEX_ID_INDEX_H_ // NOLINT
