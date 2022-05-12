// Everything related to creating the trace ID index
#ifndef  MICROSERVICES_ENV_TRACE_STORAGE_ID_INDEX_ID_INDEX_H_
#define  MICROSERVICES_ENV_TRACE_STORAGE_ID_INDEX_ID_INDEX_H_

#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <regex>
#include <vector>
#include <string>
#include <utility>
#include <map>
#include <boost/regex.hpp>
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "bloom_filter.hpp"
#include <boost/algorithm/string/regex.hpp>

namespace gcs = ::google::cloud::storage;
const char trace_struct_bucket[] = "dyntraces-snicket4";
const char hyphen[] = "-";
const char newline[] = "\n";
const int branching_factor = 10;

std::vector<std::string> split_string_by_char(const std::string& str, std::string& ch);
std::vector<std::string> generate_prefixes(time_t earliest, time_t latest);
std::vector<std::vector<std::string>> get_batches_between_timestamps(gcs::Client* client, time_t earliest, time_t latest);
bloom_filter create_bloom_filter_partial_batch(gcs::Client* client, std::string batch, time_t earliest, time_t latest);
bloom_filter create_bloom_filter_entire_batch(gcs::Client* client, std::string batch);
int make_leaf(gcs::Client* client, std::vector<std::vector<std::string>> batches, time_t start_time, time_t end_time);
int bubble_up_leaf(gcs::Client* client, time_t start_time, time_t end_time);
int create_index_bucket(gcs::Client* client);
int bloom_filter_to_storage(gcs::Client* client, std::string object_name, bloom_filter* bf);
int bubble_up_bloom_filter(gcs::Client* client, bloom_filter bf);
int update_index(gcs::Client* client, time_t last_updated);


struct Leaf {
    std::vector<std::string> batch_names;
    std::vector<bloom_filter> bloom_filters;
};

void serialize(Leaf leaf, std::ostream& os);
Leaf deserialize(std::istream& is);
bool leaf_sizes_equal(struct Leaf &leaf1, struct Leaf &leaf2);
bool batch_names_equal(struct Leaf &leaf1, struct Leaf &leaf2);
bool bloom_filters_equal(struct Leaf &leaf1, struct Leaf &leaf2);
bool leaf_equals(struct Leaf &leaf1, struct Leaf &leaf2);

#endif  // MICROSERVICES_ENV_TRACE_STORAGE_ID_INDEX_ID_INDEX_H_
