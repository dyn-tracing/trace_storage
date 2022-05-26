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


struct fetched_data {
    std::unordered_map<std::string, std::string> structural_objects_by_bn;  // [batch_name]

    std::unordered_map<
        std::string,
        std::unordered_map<
            std::string,
            opentelemetry::proto::trace::v1::TracesData>> spans_objects_by_bn_sn;  // [batch_name][service_name]
};

fetched_data fetch_data(
    traces_by_structure& structs_result,
    std::map<std::string, std::vector<std::string>>& object_name_to_trace_ids_of_interest,
    std::vector<query_condition> &conditions,
    gcs::Client* client
);
bool is_indexed(query_condition *condition, gcs::Client* client);
bool does_span_satisfy_condition(
    std::string span_id, std::string service_name,
    query_condition condition, std::string batch_name, fetched_data& evaluation_data
);
std::vector<int> get_iso_maps_indices_for_which_trace_satifies_curr_condition(
    std::string trace_id, std::string batch_name, std::vector<query_condition>& conditions,
    int curr_cond_ind, fetched_data& evaluation_data, traces_by_structure& structural_results
);
objname_to_matching_trace_ids get_traces_by_indexed_condition(
    int start_time, int end_time, query_condition *condition, gcs::Client* client);
objname_to_matching_trace_ids filter_based_on_conditions(
    objname_to_matching_trace_ids &intersection,
    traces_by_structure &structural_results,
    std::vector<query_condition> &conditions,
    struct fetched_data &fetched
);
bool does_trace_satisfy_conditions(std::string trace_id, std::string object_name, 
    std::vector<query_condition> &conditions, fetched_data& evaluation_data,
    traces_by_structure &structural_results
);

// ***************** query-related ******************************************
std::vector<std::string> get_return_value(
    objname_to_matching_trace_ids filtered, return_value ret, gcs::Client* client);
objname_to_matching_trace_ids intersect_index_results(
    std::vector<objname_to_matching_trace_ids> index_results,
    traces_by_structure structural_results);

int dummy_tests();

#endif  // GRAPH_QUERY_H_ // NOLINT
