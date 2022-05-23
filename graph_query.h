// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

/**
 * TODO: Some boost definitions (functions, classes, structs) that I overrode 
 * for extracting all the isomorphism maps, use camel casing and I have retained that
 * for consistency with the corresponding boost definitions. But we are using snake case
 * in the rest of our codebase, so maybe convert all of those overriden defs to snake_case. 
 */

#ifndef GRAPH_QUERY_H_
#define GRAPH_QUERY_H_

#include <iostream>
#include <unordered_map>
#include <utility>
#include <map>
#include <string>
#include <vector>
#include <future>
#include "query_conditions.h"
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include <boost/algorithm/string.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/vf2_sub_graph_iso.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include "get_traces_by_structure.h"
#include "common.h"


struct data_for_verifying_conditions {
	std::vector <std::vector <std::string>> service_name_for_condition_with_isomap;
	std::unordered_map<std::string, opentelemetry::proto::trace::v1::TracesData> service_name_to_respective_object;
};

std::vector<std::string> split_by_char(std::string input, std::string splitter);

std::string extract_any_trace(std::vector<std::string>& trace_ids, std::string& object_content);
opentelemetry::proto::trace::v1::TracesData read_object_and_parse_traces_data(
	std::string bucket, std::string object_name, gcs::Client* client
);
data_for_verifying_conditions get_gcs_objects_required_for_verifying_conditions(
	std::vector<query_condition> conditions, std::vector<std::unordered_map<int, int>> iso_maps,
	std::unordered_map<int, std::string> trace_node_names,
	std::unordered_map<int, std::string> query_node_names,
	std::string batch_name, std::string trace, gcs::Client* client
);
bool does_span_satisfy_condition(
	std::string span_id, std::string service_name,
	query_condition condition, data_for_verifying_conditions& verification_data
);
std::vector<int> get_iso_maps_indices_for_which_trace_satifies_condition(
	std::string trace_id, query_condition condition,
	int num_iso_maps, std::string object_content,
	data_for_verifying_conditions& verification_data, int condition_index_in_verification_data
);
std::vector<std::string> process_trace_hashes_object_and_retrieve_relevant_trace_ids(
	StatusOr<gcs::ObjectMetadata> object_metadata, trace_structure query_trace,
	int start_time, int end_time, std::vector<query_condition> conditions, gcs::Client* client);
std::string hex_str(std::string data, int len);
std::map<std::string, std::pair<int, int>> get_timestamp_map_for_trace_ids(
	std::string spans_data, std::vector<std::string> trace_ids);
std::map<std::string, std::vector<std::string>> get_root_service_to_trace_ids_map(
	std::map<std::string, std::string> trace_id_to_root_service_map);
std::map<std::string, std::string> get_trace_id_to_root_service_map(std::string object_content);
std::vector<std::string> filter_trace_ids_based_on_query_timestamp(
	std::vector<std::string> trace_ids, std::string batch_name, std::string object_content,
	int start_time, int end_time, gcs::Client* client);
std::vector<std::string> filter_trace_ids_based_on_conditions(
	std::vector<std::string> trace_ids,
	int trace_ids_start_index,
	std::string object_content,
	std::vector<query_condition> conditions,
	int num_iso_maps,
	data_for_verifying_conditions& required_data
);
void print_trace_structure(trace_structure trace);
std::string extract_trace_from_traces_object(std::string trace_id, std::string& object_content);
std::pair<int, int> extract_batch_timestamps(std::string batch_name);
std::string extract_batch_name(std::string object_name);
std::vector<std::unordered_map<int, int>> get_isomorphism_mappings(
	trace_structure candidate_trace, trace_structure query_trace);
std::vector<std::string> split_by_line(std::string input);
bool is_object_within_timespan(std::pair<int, int> batch_time, int start_time, int end_time);
std::string read_object(std::string bucket, std::string object, gcs::Client* client);
std::vector<std::string> get_trace_ids_from_trace_hashes_object(std::string object_name, gcs::Client* client);
int get_trace(std::string traceID, int start_time, int end_time, gcs::Client* client);
trace_structure morph_trace_object_to_trace_structure(std::string trace);
bool does_trace_satisfy_all_conditions(
	std::string trace_id, std::string object_content, std::vector<query_condition> conditions,
	int num_iso_maps, data_for_verifying_conditions& verification_data
);
bool is_indexed(query_condition *condition, gcs::Client* client);
std::vector<objname_to_matching_trace_ids> get_traces_by_indexed_condition(int start_time, int end_time, query_condition *condition, gcs::Client* client);
std::vector<std::string> get_return_value(std::vector<objname_to_matching_trace_ids> filtered, return_value ret, gcs::Client* client);
int dummy_tests();

#endif  // GRAPH_QUERY_H_
