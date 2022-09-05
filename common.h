/*
 * Contains various common functions for interacting with cloud storage
 * according to the format we've stored trace data.
*/

#ifndef COMMON_H_ // NOLINT
#define COMMON_H_

#include <map>
#include <string>
#include <vector>
#include <future>
#include <tuple>
#include <utility>
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include <boost/algorithm/string.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/vf2_sub_graph_iso.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>

const char BUCKET_TYPE_LABEL_KEY[] = "bucket_type";
const char BUCKET_TYPE_LABEL_VALUE_FOR_SPAN_BUCKETS[] = "microservice";
const char PROJECT_ID[] = "dynamic-tracing";
const char BUCKETS_LOCATION[] = "us-central1";

const char TRACE_STRUCT_BUCKET[] = "dyntraces-snicket2";
const char TRACE_HASHES_BUCKET[] = "tracehashes-snicket2";
const char BUCKETS_SUFFIX[] = "-snicket2";

const char TRACE_STRUCT_BUCKET_PREFIX[] = "dyntraces";
const int TRACE_ID_LENGTH = 32;
const int SPAN_ID_LENGTH = 16;
const int element_count = 10000;
const char hyphen[] = "-";
const char newline[] = "\n";
const char colon[] = ":";

constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

namespace gcs = ::google::cloud::storage;
namespace ot = opentelemetry::proto::trace::v1;

typedef std::map<std::string, std::vector<std::string>> objname_to_matching_trace_ids;

enum index_type {
    bloom,
    folder,
    none,
    not_found,
};

/// **************** pure string processing ********************************
std::vector<std::string> split_by_string(const std::string& str,  const char* ch);
std::string hex_str(const std::string &data, int len);
bool is_same_hex_str(const std::string &data, const std::string &compare);
std::string strip_from_the_end(std::string object, char stripper);
void replace_all(std::string& str, const std::string& from, const std::string& to);
void print_progress(float progress, std::string label, bool verbose);

/// *********** string processing according to system conventions **********
std::map<std::string, std::pair<int, int>> get_timestamp_map_for_trace_ids(
    const std::string &spans_data, const std::vector<std::string> &trace_ids);
bool object_could_have_out_of_bound_traces(std::pair<int, int> batch_time, int start_time, int end_time);
bool is_object_within_timespan(std::pair<int, int> batch_time, int start_time, int end_time);
std::string extract_batch_name(const std::string &object_name);
std::pair<int, int> extract_batch_timestamps(const std::string &batch_name);
std::map<std::string, std::string> get_trace_id_to_root_service_map(const std::string &object_content);
std::map<std::string, std::vector<std::string>> get_root_service_to_trace_ids_map(
    const std::map<std::string, std::string> &trace_id_to_root_service_map);
std::string extract_any_trace(std::vector<std::string>& trace_ids, std::string& object_content);
std::string extract_trace_from_traces_object(const std::string &trace_id, std::string& object_content);

/// **************** GCS processing ********************************
opentelemetry::proto::trace::v1::TracesData read_object_and_parse_traces_data(
    const std::string &bucket, const std::string &object_name, gcs::Client* client);
std::string read_object(const std::string &bucket, const std::string &object, gcs::Client* client);

std::vector<std::string> filter_trace_ids_based_on_query_timestamp(
    const std::vector<std::string> &trace_ids,
    const std::string &batch_name,
    const std::string &object_content,
    int start_time,
    int end_time,
    gcs::Client* client);

std::vector<std::string> get_spans_buckets_names(gcs::Client* client);
std::string get_index_bucket_name(std::string property_name);

#endif  // COMMON_H_ // NOLINT
