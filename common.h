/*
 * Contains various common functions for interacting with cloud storage
 * according to the format we've stored trace data.
*/

#ifndef COMMON_H_                                                            
#define COMMON_H_ 

#include <map>                                                                  
#include <string>                                                               
#include <vector>                                                               
#include <future>                                                               
#include "google/cloud/storage/client.h"                                        
#include "opentelemetry/proto/trace/v1/trace.pb.h" 
#include <boost/algorithm/string.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/vf2_sub_graph_iso.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>

const char TRACE_STRUCT_BUCKET_PREFIX[] = "dyntraces";
const char SERVICES_BUCKETS_SUFFIX[] = "-snicket4";
const int TRACE_ID_LENGTH = 32;
const int SPAN_ID_LENGTH = 16;
const int element_count = 10000;
const char hyphen[] = "-";
const char newline[] = "\n";
const char colon[] = ":";

namespace gcs = ::google::cloud::storage;
typedef std::tuple<std::string, std::vector<std::string>> objname_to_matching_trace_ids;



/// **************** pure string processing ********************************
std::vector<std::string> split_by_string(std::string& str,  const char* ch);
std::string hex_str(std::string data, int len);
std::string strip_from_the_end(std::string object, char stripper);

/// *********** string processing according to system conventions **********
std::map<std::string, std::pair<int, int>> get_timestamp_map_for_trace_ids(
    std::string spans_data, std::vector<std::string> trace_ids);
bool is_object_within_timespan(std::pair<int, int> batch_time, int start_time, int end_time);
std::string extract_batch_name(std::string object_name);
std::pair<int, int> extract_batch_timestamps(std::string batch_name);
std::map<std::string, std::string> get_trace_id_to_root_service_map(std::string object_content);
std::map<std::string, std::vector<std::string>> get_root_service_to_trace_ids_map(
    std::map<std::string, std::string> trace_id_to_root_service_map);
std::string extract_any_trace(std::vector<std::string>& trace_ids, std::string& object_content);
std::string extract_trace_from_traces_object(std::string trace_id, std::string& object_content);

/// **************** GCS processing ********************************
opentelemetry::proto::trace::v1::TracesData read_object_and_parse_traces_data(
    std::string bucket, std::string object_name, gcs::Client* client);
std::string read_object(std::string bucket, std::string object, gcs::Client* client);

std::vector<std::string> filter_trace_ids_based_on_query_timestamp(
    std::vector<std::string> trace_ids,
    std::string batch_name,
    std::string object_content,
    int start_time,
    int end_time,
    gcs::Client* client);

#endif // COMMON_H_
