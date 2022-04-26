#include "google/cloud/storage/client.h"
#include <boost/algorithm/string.hpp>
#include <boost/graph/vf2_sub_graph_iso.hpp>
#include <iostream>

const std::string trace_struct_bucket = "dyntraces-snicket2";
const std::string trace_hashes_bucket = "tracehashes-snicket2";

namespace gcs = ::google::cloud::storage;
namespace bg = boost::graph;

std::string extract_trace_from_traces_object(std::string trace_id, std::string object_content);
std::pair<int, int> extract_batch_timestamps(std::string batch_name); 
std::string extract_batch_name(std::string object_name);
bool does_trace_structure_conform_to_graph_query( std::string traces_object, std::string trace_id, std::string parent_service, std::string child_service, gcs::Client* client);
std::vector<std::string> split_by_line(std::string input);
bool is_object_within_timespan(std::string object_name, int start_time, int end_time);
std::string read_object(std::string bucket, std::string object, gcs::Client* client);
std::vector<std::string> get_trace_ids_from_trace_hashes_object(std::string object_name, gcs::Client* client);
int get_trace(std::string traceID, int start_time, int end_time, gcs::Client* client); 
int get_traces_by_structure(std::string parent_service, std::string child_service, int start_time, int end_time, gcs::Client* client);
int dummy_tests();