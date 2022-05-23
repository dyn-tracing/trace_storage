// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

/**
 * TODO: Some boost definitions (functions, classes, structs) that I overrode 
 * for extracting all the isomorphism maps, use camel casing and I have retained that
 * for consistency with the corresponding boost definitions. But we are using snake case
 * in the rest of our codebase, so maybe convert all of those overriden defs to snake_case. 
 */

#ifndef GRAPH_QUERY_H_ // NOLINT
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


std::vector<std::string> query(
    trace_structure query_trace, int start_time, int end_time,
    std::vector<query_condition> conditions, return_value ret, gcs::Client* client);

// ****************** conditions-related ********************************
struct data_for_verifying_conditions {
	std::vector <std::vector <std::string>> service_name_for_condition_with_isomap;
	std::unordered_map<std::string, opentelemetry::proto::trace::v1::TracesData> service_name_to_respective_object;
};

struct fetched_data {
    // TODO(haseeb)
};

data_for_verifying_conditions get_gcs_objects_required_for_verifying_conditions(
	std::vector<query_condition> conditions, std::vector<std::unordered_map<int, int>> iso_maps,
	std::unordered_map<int, std::string> trace_node_names,
	std::unordered_map<int, std::string> query_node_names,
	std::string batch_name, std::string trace, gcs::Client* client
);
bool is_indexed(query_condition *condition, gcs::Client* client);
bool does_span_satisfy_condition(
	std::string span_id, std::string service_name,
	query_condition condition, data_for_verifying_conditions& verification_data
);
std::vector<int> get_iso_maps_indices_for_which_trace_satifies_condition(
	std::string trace_id, query_condition condition,
	int num_iso_maps, std::string object_content,
	data_for_verifying_conditions& verification_data, int condition_index_in_verification_data
);
std::vector<std::string> filter_trace_ids_based_on_conditions(
	std::vector<std::string> trace_ids,
	int trace_ids_start_index,
	std::string object_content,
	std::vector<query_condition> conditions,
	int num_iso_maps,
	data_for_verifying_conditions& required_data
);
bool does_trace_satisfy_all_conditions(
	std::string trace_id, std::string object_content, std::vector<query_condition> conditions,
	int num_iso_maps, data_for_verifying_conditions& verification_data
);
std::vector<objname_to_matching_trace_ids> get_traces_by_indexed_condition(
    int start_time, int end_time, query_condition *condition, gcs::Client* client);
std::vector<objname_to_matching_trace_ids> filter_based_on_conditions(
        std::vector<objname_to_matching_trace_ids> &intersection,
        traces_by_structure &structural_results,
        std::vector<query_condition> &conditions, gcs::Client* client);

// ***************** query-related ******************************************
std::vector<std::string> get_return_value(
    std::vector<objname_to_matching_trace_ids> filtered, return_value ret, gcs::Client* client);
std::vector<objname_to_matching_trace_ids> intersect_index_results(
    std::vector<std::vector<objname_to_matching_trace_ids>> index_results,
    traces_by_structure structural_results);
int dummy_tests();

#endif  // GRAPH_QUERY_H_ // NOLINT
