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

const char SERVICES_BUCKETS_SUFFIX[] = "-snicket4";
const int TRACE_ID_LENGTH = 32;
const int SPAN_ID_LENGTH = 16;

namespace gcs = ::google::cloud::storage;

std::vector<std::string> split_by_char(std::string input, std::string splitter);
std::vector<std::string> split_by_line(std::string input);
std::string hex_str(std::string data, int len);
std::map<std::string, std::pair<int, int>> get_timestamp_map_for_trace_ids(
    std::string spans_data, std::vector<std::string> trace_ids);
std::vector<std::string> split_by_string(std::string input, std::string splitter);
opentelemetry::proto::trace::v1::TracesData read_object_and_parse_traces_data(
    std::string bucket, std::string object_name, gcs::Client* client
);

std::string read_object(std::string bucket, std::string object, gcs::Client* client);
bool is_object_within_timespan(std::pair<int, int> batch_time, int start_time, int end_time);
std::string strip_from_the_end(std::string object, char stripper);
std::string extract_batch_name(std::string object_name);
std::pair<int, int> extract_batch_timestamps(std::string batch_name);

std::vector<std::string> filter_trace_ids_based_on_query_timestamp(
    std::vector<std::string> trace_ids,
    std::string batch_name,
    std::string object_content,
    int start_time,
    int end_time,
    gcs::Client* client);

std::map<std::string, std::string> get_trace_id_to_root_service_map(std::string object_content);
std::map<std::string, std::vector<std::string>> get_root_service_to_trace_ids_map(
    std::map<std::string, std::string> trace_id_to_root_service_map);
std::string extract_any_trace(std::vector<std::string>& trace_ids, std::string& object_content);
std::string extract_trace_from_traces_object(std::string trace_id, std::string& object_content);


#endif // COMMON_H_
