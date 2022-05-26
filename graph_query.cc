#include "graph_query.h"


std::vector<std::string> query(
    trace_structure query_trace, int start_time, int end_time,
    std::vector<query_condition> conditions, return_value ret, gcs::Client* client) {

    // first, get all matches to indexed query conditions
    // note that structural is always indexed

    std::future<traces_by_structure> struct_filter_obj = std::async(std::launch::async,
        get_traces_by_structure,
        query_trace, start_time, end_time, client);

    std::vector<std::future<objname_to_matching_trace_ids>> index_results_futures;
    for (int i=0; i < conditions.size(); i++) {
        if (is_indexed(&conditions[i], client)) {
            index_results_futures.push_back(std::async(std::launch::async, get_traces_by_indexed_condition,
            start_time, end_time, &conditions[i], client));
        }
    }

    std::vector<objname_to_matching_trace_ids> index_results;
    for (int i=0; i < index_results_futures.size(); i++) {
        index_results.push_back(index_results_futures[i].get());
    }
    auto struct_results = struct_filter_obj.get();

    objname_to_matching_trace_ids intersection = intersect_index_results(index_results, struct_results);

    fetched_data fetched = fetch_data(
        struct_results,
        intersection,
        conditions,
        client);

    objname_to_matching_trace_ids filtered = filter_based_on_conditions(
        intersection, struct_results, conditions, fetched);

    return get_return_value(filtered, ret, client);
}

bool is_indexed(query_condition *condition, gcs::Client* client) {
    // TODO(jessica)
    return false;
}

objname_to_matching_trace_ids get_traces_by_indexed_condition(
    int start_time, int end_time, query_condition *condition, gcs::Client* client) {
    // TODO(jessica)
    objname_to_matching_trace_ids to_return;
    return to_return;
}

objname_to_matching_trace_ids filter_based_on_conditions(
    objname_to_matching_trace_ids &intersection,
    traces_by_structure &structural_results,
    std::vector<query_condition> &conditions,
    struct fetched_data &fetched
) {
    objname_to_matching_trace_ids to_return;
    for (const auto &object_to_trace : intersection) {
        for (int i=0; i < object_to_trace.second.size(); i++) {
            if (does_trace_satisfy_conditions(object_to_trace.second[i], object_to_trace.first, conditions, fetched, structural_results)) {
                to_return[object_to_trace.first].push_back(object_to_trace.second[i]);
            }
        }
    }
    return to_return;
}

objname_to_matching_trace_ids intersect_index_results(
    std::vector<objname_to_matching_trace_ids> index_results,
    traces_by_structure structural_results) {
    // TODO(jessica)
}

std::vector<std::string> get_return_value(
    objname_to_matching_trace_ids filtered, return_value ret, gcs::Client* client) {
    // TODO(jessica)
}

/**
 * Fetches data that is required for evaluating conditions. 
 */
fetched_data fetch_data(
    traces_by_structure& structs_result,
    std::map<std::string, std::vector<std::string>>& object_name_to_trace_ids_of_interest,
    std::vector<query_condition> &conditions,
    gcs::Client* client
) {
    fetched_data response;

    std::string trace_structure_bucket_prefix(TRACE_STRUCT_BUCKET_PREFIX);
    std::string buckets_suffix(BUCKETS_SUFFIX);

    for (auto& ontii_ele : object_name_to_trace_ids_of_interest) {
        auto batch_name = ontii_ele.first;
        auto trace_ids = ontii_ele.second;
        if (trace_ids.size() < 1) {
            continue;
        }

        if (response.structural_objects_by_bn.find(batch_name) == response.structural_objects_by_bn.end()) {
            response.structural_objects_by_bn[batch_name] = read_object(
                trace_structure_bucket_prefix+buckets_suffix, batch_name, client);
        }

        auto iso_map_indices = structs_result.trace_id_to_isomap_indices[trace_ids[0]];
        for (auto curr_condition : conditions) {
            for (auto curr_iso_map_ind : iso_map_indices) {
                auto trace_node_names_ind = structs_result.iso_map_to_trace_node_names[curr_iso_map_ind];
                auto trace_node_index = structs_result.iso_maps[curr_iso_map_ind][curr_condition.node_index];
                auto condition_service = structs_result.trace_node_names[trace_node_names_ind][trace_node_index];

                /**
                 * @brief while parallelizing, just make 
                 * response.spans_objects_by_bn_sn[batch_name][service_name_without_hash_id] = true
                 * sort of map first and then make asynchronous calls on em. cuz there can be duplicate calls to
                 * spans_objects_by_bn_sn[batch_name][service_name_without_hash_id], so we dont wanna fetch same obj
                 * more than once.
                 */
                auto service_name_without_hash_id = split_by_string(condition_service, ":")[0];
                auto trace_data = read_object_and_parse_traces_data(
                    service_name_without_hash_id+BUCKETS_SUFFIX, batch_name, client);
                response.spans_objects_by_bn_sn[batch_name][service_name_without_hash_id] = trace_data;
            }
        }
    }

    return response;
}

bool does_trace_satisfy_conditions(std::string trace_id, std::string object_name, 
    std::vector<query_condition> &conditions, fetched_data& evaluation_data,
    traces_by_structure &structural_results
) {
    std::vector<std::vector<int>> satisfying_iso_map_indices_for_all_conditions;
    for (int curr_cond_ind = 0; curr_cond_ind < conditions.size(); curr_cond_ind++) {
        satisfying_iso_map_indices_for_all_conditions.push_back(
            get_iso_maps_indices_for_which_trace_satifies_curr_condition(
                trace_id, object_name, conditions, curr_cond_ind, evaluation_data, structural_results));
    }

    /**
     * @brief All the biz below is for checking whether there exists a single isomap
     * which lead to true evaluation of all conditions. 
     * TODO: separate it out in a function. 
     */
    auto relevant_iso_maps_indices = structural_results.trace_id_to_isomap_indices[trace_id];
    std::unordered_map<int, int> iso_map_to_num_of_satisfied_conditions;
    for (auto i : relevant_iso_maps_indices) {
        iso_map_to_num_of_satisfied_conditions[i] = 0;
    }

    for(int i = 0; i < satisfying_iso_map_indices_for_all_conditions.size(); i++) {
        auto satisfying_iso_map_indices = satisfying_iso_map_indices_for_all_conditions[i];
        for (auto& iso_map_ind : satisfying_iso_map_indices) {
            iso_map_to_num_of_satisfied_conditions[iso_map_ind] += 1;
        }
    }

    for (auto i : relevant_iso_maps_indices) {
        if (iso_map_to_num_of_satisfied_conditions[i] >= conditions.size()) {
            return true;
        }
    }
    return false;
}

std::vector<int> get_iso_maps_indices_for_which_trace_satifies_curr_condition(
    std::string trace_id, std::string batch_name, std::vector<query_condition>& conditions,
    int curr_cond_ind, fetched_data& evaluation_data, traces_by_structure& structural_results
) {
    std::vector<int> satisfying_iso_map_indices;

    auto relevant_iso_maps_indices = structural_results.trace_id_to_isomap_indices[trace_id];
    for (auto curr_iso_map_ind : relevant_iso_maps_indices) {
        std::string trace = extract_trace_from_traces_object(trace_id,
            evaluation_data.structural_objects_by_bn[batch_name]);
        std::vector<std::string> trace_lines = split_by_string(trace, newline);

        /**
         * @brief Get condition_service name here, somehow
         * 
         */
        auto trace_node_names_ind = structural_results.iso_map_to_trace_node_names[curr_iso_map_ind];
        auto trace_node_index = structural_results.iso_maps[curr_iso_map_ind][conditions[curr_cond_ind].node_index];
        auto condition_service = structural_results.trace_node_names[trace_node_names_ind][trace_node_index];

        for (auto line : trace_lines) {
            if (line.find(condition_service) != std::string::npos) {
                auto span_info = split_by_string(line, colon);
                auto span_id = span_info[1];
                auto service_name = span_info[2];  // this service_name is without span level hash
                if (true == does_span_satisfy_condition(
                    span_id, service_name, conditions[curr_cond_ind], batch_name, evaluation_data)
                ) {
                    satisfying_iso_map_indices.push_back(curr_iso_map_ind);
                }
            }
        }
    }

    return satisfying_iso_map_indices;
}

bool does_span_satisfy_condition(
    std::string span_id, std::string service_name,
    query_condition condition, std::string batch_name, fetched_data& evaluation_data
) {
    if (evaluation_data.spans_objects_by_bn_sn.find(batch_name) == evaluation_data.spans_objects_by_bn_sn.end()
    || evaluation_data.spans_objects_by_bn_sn[batch_name].find(
        service_name) == evaluation_data.spans_objects_by_bn_sn[batch_name].end()) {
            std::cerr << "Error in does_span_satisfy_condition: Required data not found!" << std::endl;
            exit(1);
    }

    opentelemetry::proto::trace::v1::TracesData* trace_data = &(
        evaluation_data.spans_objects_by_bn_sn[batch_name][service_name]);

    const opentelemetry::proto::trace::v1::Span* sp;
    for (int i=0; i < trace_data->resource_spans(0).scope_spans(0).spans_size(); i++) {
        sp = &(trace_data->resource_spans(0).scope_spans(0).spans(i));

        std::string current_span_id = hex_str(sp->span_id(), sp->span_id().length());
        if (current_span_id == span_id) {
            return does_condition_hold(sp, condition);
        }
    }

    return false;
}

int dummy_tests() {
    return 0;
}
