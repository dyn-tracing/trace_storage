#ifndef FOLDERS_INDEX_QUERY_H_
#define FOLDERS_INDEX_QUERY_H_

#include <iostream>
#include <unordered_map>
#include <utility>
#include <map>
#include <string>
#include <vector>
#include <future>

#include "query_conditions.h"
#include "folders_index/trace_attributes.h"
#include "google/cloud/storage/client.h"
#include "graph_query.h"

namespace gcs = ::google::cloud::storage;
using ::google::cloud::StatusOr;

std::unordered_map<std::string, std::vector<std::string>> get_obj_name_to_trace_ids_map_from_folders_index(
	std::string attr_key, std::string attr_val, gcs::Client* client
);
std::unordered_map<std::string, std::vector<std::string>>
process_findex_object_and_retrieve_obj_name_to_trace_ids_map(
	std::string findex_obj_name, std::string bucket_name, gcs::Client* client
);
void print_folders_index_query_res(std::unordered_map<std::string, std::vector<std::string>> res);

#endif  // FOLDERS_INDEX_QUERY_H_