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
        struct_results.iso_maps,
        struct_results.trace_node_names,
        query_trace.node_names,
        intersection,
        struct_results.trace_id_to_isomap_indices,
        struct_results.iso_map_to_trace_node_names,
        conditions,
        client);

    objname_to_matching_trace_ids filtered = filter_based_on_conditions(
        intersection, struct_results, conditions, query_trace, fetched, client);

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
    trace_structure &query_trace,
    struct fetched_data &fetched,
    gcs::Client* client
) {
    objname_to_matching_trace_ids to_return;
    for (const auto &object_to_trace : intersection) {
        for (int i=0; i < object_to_trace.second.size(); i++) {
            std::vector<std::unordered_map<int, int>> isomaps;
            std::vector<int> isomap_indices = structural_results.trace_id_to_isomap_indices[object_to_trace.second[i]];
            for (int k=0; k < isomap_indices.size(); k++) {
                isomaps.push_back(structural_results.iso_maps[isomap_indices[k]]);
            }
            if (does_trace_satisfy_conditions(object_to_trace.second[i], object_to_trace.first,
                isomaps, conditions, fetched)) {
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

fetched_data fetch_data(
    std::vector<std::unordered_map<int, int>> &iso_maps,
    std::vector<std::unordered_map<int, std::string>> trace_node_names,
    std::unordered_map<int, std::string> query_node_names,
    std::map<std::string, std::vector<std::string>> object_name_to_trace_ids_of_interest,
    std::map<std::string, std::vector<int>> trace_id_to_isomap_indices,
    std::map<int, int> iso_map_to_trace_node_names,
    std::vector<query_condition> &conditions,
    gcs::Client* client
) {
    /**
        std::unordered_map<std::string, std::string> structural_objects_by_bn;  // [batch_name]

        std::unordered_map<
            std::string,
            std::vector<std::vector<std::string>>> service_names_by_p_ci_ii  // [prefix][condition_ind][iso_map_ind];

        std::unordered_map<
            std::string,
            std::unordered_map<
                std::string,
                opentelemetry::proto::trace::v1::TracesData>> spans_objects_by_bn_sn;  // [batch_name][service_name]
     */

    fetched_data response;

    auto batch_names = extract_all_batch_names(object_name_to_trace_ids_of_interest);
    response.structural_objects_by_bn = get_structural_objects_by_bn_map(batch_names);


    for (auto& ontii_ele : object_name_to_trace_ids_of_interest) {
        auto object_name = ontii_ele.first;
        auto splitted_object_name = split_by_string(object_name, "/");
        auto trace_hashes_prefix = splitted_object_name[0];
        auto batch_name = splitted_object_name[1];
        auto trace_ids = ontii_ele.second;

        std::string any_trace_of_current_prefix = trace_ids[0];
        auto iso_map_indices = trace_id_to_isomap_indices[any_trace_of_current_prefix];
        
        for (auto curr_condition : conditions) {
            std::vector <std::string> iso_map_to_service;
            for (auto curr_iso_map_ind : iso_map_indices) {
                auto trace_node_names_ind = iso_map_to_trace_node_names[curr_iso_map_ind];
                auto trace_node_index = iso_maps[curr_iso_map_ind][curr_condition.node_index];
                auto condition_service = trace_node_names[trace_node_names_ind][trace_node_index];
                iso_map_to_service.push_back(condition_service);

                auto service_name_without_hash_id = split_by_string(condition_service, ":")[0];
                auto trace_data = read_object_and_parse_traces_data(service_name_without_hash_id+SERVICES_BUCKETS_SUFFIX, batch_name);

                // TODO:
                // store this trace_data in spans_objects_by_bn_sn
                // make sure that read_object_and_parse_traces_data does not get called for same object again
                // once this is done, make sure, end to end thing (upto and include does_span_satisfy_condition is okay)
                // then make it parallele or even wait for jessica's code so that it can be tested before going to parallel

            }
            response.service_names_by_p_ci_ii[prefix].push_back(iso_map_to_service);
        }
    }

}

bool does_trace_satisfy_conditions(std::string trace_id, std::string object_name,
    std::vector<std::unordered_map<int, int>>& iso_maps, std::vector<query_condition> &conditions,
    fetched_data& evaluation_data
) {
    std::vector<std::vector<int>> satisfying_iso_map_indices_for_all_conditions;
    for (int curr_cond_ind = 0; curr_cond_ind < conditions.size(); curr_cond_ind++) {
        satisfying_iso_map_indices_for_all_conditions.push_back(
            get_iso_maps_indices_for_which_trace_satifies_curr_condition(
                trace_id, object_name, conditions, curr_cond_ind, iso_maps, evaluation_data));
    }

    std::unordered_map<int, int> iso_map_to_num_of_satisfied_conditions;
    for (int i = 0; i < iso_maps.size(); i++) {
        iso_map_to_num_of_satisfied_conditions[i] = 0;
    }

    for(int i = 0; i < satisfying_iso_map_indices_for_all_conditions.size(); i++) {
        auto satisfying_iso_map_indices = satisfying_iso_map_indices_for_all_conditions[i];
        for (auto& iso_map_ind : satisfying_iso_map_indices) {
            iso_map_to_num_of_satisfied_conditions[iso_map_ind] += 1;
        }
    }

    for (int i = 0; i < iso_maps.size(); i++) {
        if (iso_map_to_num_of_satisfied_conditions[i] >= conditions.size()) {
            return true;
        }
    }
    return false;
}

opentelemetry::proto::trace::v1::TracesData read_object_and_parse_traces_data(
	std::string bucket, std::string object_name, gcs::Client* client
) {
	auto data = read_object(bucket, object_name, client);
	opentelemetry::proto::trace::v1::TracesData trace_data;
	if (data == "") {
		return trace_data;
	}

	bool ret = trace_data.ParseFromString(data);
	if (false == ret) {
		std::cerr << "Error in read_object_and_parse_traces_data:ParseFromString" << std::endl;
		exit(1);
	}

	return trace_data;
}

std::vector<int> get_iso_maps_indices_for_which_trace_satifies_curr_condition(
    std::string trace_id, std::string object_name, std::vector<query_condition>& conditions,
    int curr_cond_ind, std::vector<std::unordered_map<int, int>>& iso_maps, fetched_data& evaluation_data
) {
    auto splitted_object_name = split_by_string(object_name, "/")[0];
    auto prefix = splitted_object_name[0];
    auto batch_name =splitted_object_name[1];

    std::vector<int> satisfying_iso_map_indices;
    for (int curr_iso_map_ind = 0; curr_iso_map_ind < iso_maps.size(); curr_iso_map_ind++) {
        std::string trace = extract_trace_from_traces_object(trace_id,
            evaluation_data.structural_objects_by_bn[batch_name]);
        std::vector<std::string> trace_lines = split_by_string(trace, newline);

        auto condition_service = evaluation_data.service_names_by_p_ci_ii[prefix][curr_cond_ind][curr_iso_map_ind];
        for (auto line : trace_lines) {
            if (line.find(condition_service) != std::string::npos) {
                auto span_info = split_by_string(line, colon);
                auto span_id = span_info[1];
                auto service_name = span_info[2];
                if (true == does_span_satisfy_condition(
                    span_id, service_name, conditions[curr_cond_ind], batch_name, evaluation_data)
                ) {
                    satisfying_iso_map_indices.push_back(curr_iso_map_ind);
                }
                break;
            }
        }
    }

    return satisfying_iso_map_indices;
}

bool does_span_satisfy_condition(
    std::string span_id, std::string service_name,
    query_condition condition, std::string batch_name, fetched_data& evaluation_data
) {
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
