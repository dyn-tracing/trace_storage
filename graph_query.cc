#include "graph_query.h"


std::vector<std::string> query(
    trace_structure query_trace, int start_time, int end_time,
    std::vector<query_condition> conditions, return_value ret, bool verbose, gcs::Client* client) {
    // clean input a little bit
    std::vector<std::string> empty;
    if (end_time < start_time) {
        std::cerr << "end time is less than start time;  aborting query";
        return empty;
    }
    if (query_trace.num_nodes != query_trace.node_names.size()) {
        std::cerr << "num nodes does not match number of node names given;  aborting query";
        return empty;
    }

    // first, get all matches to indexed query conditions
    // note that structural is always indexed

    std::future<traces_by_structure> struct_filter_obj = std::async(std::launch::async,
        get_traces_by_structure,
        query_trace, start_time, end_time, client);

    std::vector<std::future<objname_to_matching_trace_ids>> index_results_futures;
    for (int i=0; i < conditions.size(); i++) {
        index_type i_type = is_indexed(&conditions[i], client);
        if (i_type != none) {
            index_results_futures.push_back(std::async(std::launch::async, get_traces_by_indexed_condition,
            start_time, end_time, &conditions[i], i_type, client));
        }
    }
    print_progress(0, "Retrieving indices", verbose);

    size_t irf_size = index_results_futures.size();
    std::vector<objname_to_matching_trace_ids> index_results;
    for (int i=0; i < irf_size; i++) {
        index_results.push_back(index_results_futures[i].get());
        print_progress((i+1.0)/(irf_size+1.0), "Retrieving indices", verbose);
    }
    auto struct_results = struct_filter_obj.get();
    print_progress(1, "Retrieving indices", verbose);
    std::cout << std::endl;

    objname_to_matching_trace_ids intersection = intersect_index_results(index_results, struct_results, verbose);

    fetched_data fetched = fetch_data(
        struct_results,
        intersection,
        conditions,
        client);

    std::cout << "fetched data" << std::endl;

    auto filtered = filter_based_on_conditions(
        intersection, struct_results, conditions, fetched, ret);

    std::cout << "filtered based on conditions" << std::endl;
    boost::posix_time::ptime start, stop;
    start = boost::posix_time::microsec_clock::local_time();
    auto returned = get_return_value(filtered, ret, fetched, query_trace, client);
    stop = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration dur = stop - start;
    int64_t milliseconds = dur.total_milliseconds();
    std::cout << "Time taken: " << milliseconds << std::endl;
    std::cout << "len returned is " << returned.size() << std::endl;
    return returned;
}

index_type is_indexed(query_condition *condition, gcs::Client* client) {
    std::string bucket_name = condition->property_name;
    replace_all(bucket_name, ".", "-");
    StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->GetBucketMetadata(bucket_name);
    if (bucket_metadata.status().code() == ::google::cloud::StatusCode::kNotFound) {
        return none;
    }
    if (!bucket_metadata) {
        std::cout << "in error within is_indexed" << std::endl << std::flush;
        throw std::runtime_error(bucket_metadata.status().message());
    }
    for (auto const& kv : bucket_metadata->labels()) {
        if (kv.first == "bucket_type") {
            if (kv.second == "bloom_index") {
                return bloom;
            } else if (kv.first == "folder_index") {
                return folder;
            }
        }
    }
    return not_found;
}

objname_to_matching_trace_ids get_traces_by_indexed_condition(
    int start_time, int end_time, query_condition *condition, index_type ind_type, gcs::Client* client) {
    switch (ind_type) {
        case bloom: {
            assert(condition->comp == Equal_to);
            std::string bucket_name = condition->property_name;
            replace_all(bucket_name, ".", "-");
            return query_bloom_index_for_value(client, condition->node_property_value, bucket_name);
        }
        case folder: {
            return get_obj_name_to_trace_ids_map_from_folders_index(
            condition->property_name, condition->node_property_value, client);
        }
    }
}

std::tuple<objname_to_matching_trace_ids, std::map<std::string, iso_to_span_id>> filter_based_on_conditions(
    objname_to_matching_trace_ids &intersection,
    traces_by_structure &structural_results,
    std::vector<query_condition> &conditions,
    struct fetched_data &fetched,
    return_value ret
) {
    objname_to_matching_trace_ids to_return_traces;
    std::map<std::string, iso_to_span_id> trace_id_to_span_id_mappings;
    for (const auto &object_to_trace : intersection) {
        for (int i=0; i < object_to_trace.second.size(); i++) {
            iso_to_span_id res_ii_to_ni_to_si = does_trace_satisfy_conditions(
                object_to_trace.second[i], object_to_trace.first, conditions, fetched, structural_results, ret);
            //  res_ii_to_ni_to_si to be used by Jessica
            if (res_ii_to_ni_to_si.size() > 0) {
                to_return_traces[object_to_trace.first].push_back(object_to_trace.second[i]);
                trace_id_to_span_id_mappings[object_to_trace.second[i]] = res_ii_to_ni_to_si;
            }
        }
    }
    return std::make_tuple(to_return_traces, trace_id_to_span_id_mappings);
}

objname_to_matching_trace_ids intersect_index_results(
    std::vector<objname_to_matching_trace_ids> index_results,
    traces_by_structure &structural_results, bool verbose) {
    // Easiest solution is just keep a count
    // Eventually we should parallelize this, but I'm not optimizing it
    // until we measure the rest of the code
    // Premature optimization is of the devil and all that.
    print_progress(0, "Intersecting results", verbose);
    std::map<std::tuple<std::string, std::string>, int> count;
    for (int i=0; i < index_results.size(); i++) {
        print_progress(i/index_results.size(), "Intersecting results", verbose);
        for (auto const &obj_to_id : index_results[i]) {
            std::string object = obj_to_id.first;
            for (int j=0; j < obj_to_id.second.size(); j++) {
                count[std::make_tuple(object, obj_to_id.second[j])] += 1;
            }
        }
    }

    std::map<int, std::string> ind_to_trace_id;
    std::map<int, std::string> ind_to_obj;
    for (int i=0; i < structural_results.trace_ids.size(); i++) {
        ind_to_trace_id[i] = structural_results.trace_ids[i];
    }
    for (int i=0; i < structural_results.object_names.size(); i++) {
        ind_to_obj[i] = structural_results.object_names[i];
    }
    for (auto const &obj_to_id : structural_results.object_name_to_trace_ids_of_interest) {
        std::string obj = ind_to_obj[obj_to_id.first];
        for (int j=0; j < obj_to_id.second.size(); j++) {
            count[std::make_tuple(obj, ind_to_trace_id[obj_to_id.second[j]])] += 1;
        }
    }

    int goal_num = index_results.size() + 1;
    objname_to_matching_trace_ids to_return;
    for (auto const &pair : count) {
        if (pair.second == goal_num) {
            auto object = std::get<0>(pair.first);
            auto trace_id = std::get<1>(pair.first);
            to_return[object].push_back(trace_id);
        }
    }
    print_progress(1, "Intersecting results", verbose);
    std::cout << std::endl;
    return to_return;
}

std::string get_return_value_from_traces_data(
    opentelemetry::proto::trace::v1::TracesData *trace_data,
    std::string &span_to_find,
    return_value &ret
) {
     int sp_size = trace_data->resource_spans(0).scope_spans(0).spans_size();
     for (int i=0; i < sp_size; i++) {
        const opentelemetry::proto::trace::v1::Span *sp =
            &trace_data->resource_spans(0).scope_spans(0).spans(i);
        auto span_id = sp->opentelemetry::proto::trace::v1::Span::span_id();
        if (is_same_hex_str(span_id, span_id.size(), span_to_find)) {
            return get_value_as_string(sp, ret.func, ret.type);
        }
    }
    std::cerr << "didn't find the span " << span_to_find << " I was looking for " << std::endl << std::flush;
    return "";
}
std::vector<std::string> get_return_value(
    std::tuple<objname_to_matching_trace_ids, std::map<std::string, iso_to_span_id>> &filtered,
    return_value ret, fetched_data &data, trace_structure &query_trace, gcs::Client* client) {
    std::vector<std::string> to_return;

    for (auto const &obj_to_trace_ids : std::get<0>(filtered)) {
        std::string object = obj_to_trace_ids.first;
        for (int i=0; i < obj_to_trace_ids.second.size(); i++) {
            std::string trace_id = obj_to_trace_ids.second[i];
            // for each trace id, there may be multiple isomaps
            for (auto & ii_ni_sp : std::get<1>(filtered)[trace_id]) {
                std::string span_id_to_find = ii_ni_sp.second[ret.node_index];
                std::string service_name = query_trace.node_names[ret.node_index];

                if (data.spans_objects_by_bn_sn[object].find(service_name) !=
                    data.spans_objects_by_bn_sn[object].end()) {
                     opentelemetry::proto::trace::v1::TracesData* trace_data =
                        &data.spans_objects_by_bn_sn[object][service_name];
                     to_return.push_back(get_return_value_from_traces_data(trace_data, span_id_to_find, ret));
                } else {
                    // we need to retrieve the data, and then we can iterate through and get return val
                    std::string contents = read_object(service_name+BUCKETS_SUFFIX, object, client);
                    opentelemetry::proto::trace::v1::TracesData trace_data;
                    trace_data.ParseFromString(contents);
                    to_return.push_back(get_return_value_from_traces_data(&trace_data, span_id_to_find, ret));
                }
            }
        }
    }
    return to_return;
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

std::map<int, std::map<int, std::string>> does_trace_satisfy_conditions(std::string trace_id, std::string object_name,
    std::vector<query_condition> &conditions, fetched_data& evaluation_data,
    traces_by_structure &structural_results, return_value ret
) {
    // isomap_index_to_node_index_to_span_id -> ii_to_ni_to_si
    std::vector<std::map<int, std::map<int, std::string>>> ii_to_ni_to_si_data_for_all_conditions;
    for (int curr_cond_ind = 0; curr_cond_ind < conditions.size(); curr_cond_ind++) {
        ii_to_ni_to_si_data_for_all_conditions.push_back(
            get_iso_maps_indices_for_which_trace_satifies_curr_condition(
                trace_id, object_name, conditions, curr_cond_ind, evaluation_data, structural_results, ret));
    }

    std::map<int, std::map<int, std::string>> aggregate_result;
    std::map<int, int> iso_map_to_satisfied_conditions_map;
    for (auto vec_ele : ii_to_ni_to_si_data_for_all_conditions) {
        for (auto ele : vec_ele) {
            auto iso_map_index = ele.first;
            auto ni_to_si_map = ele.second;

            for (auto ni_to_si_ele : ni_to_si_map) {
                aggregate_result[iso_map_index][ni_to_si_ele.first] = ni_to_si_ele.second;
            }

            if (iso_map_to_satisfied_conditions_map.find(iso_map_index) == iso_map_to_satisfied_conditions_map.end()) {
                iso_map_to_satisfied_conditions_map[iso_map_index] = 1;
            } else {
                iso_map_to_satisfied_conditions_map[iso_map_index] += 1;
            }
        }
    }

    std::map<int, std::map<int, std::string>> response;
    for (auto ele : aggregate_result) {
        if (iso_map_to_satisfied_conditions_map[ele.first] >= conditions.size()) {
            response[ele.first].insert(ele.second.begin(), ele.second.end());
        }
    }

    return response;
}

std::string get_service_name_for_node_index(
    traces_by_structure& structural_results, int iso_map_index, int node_index
) {
    auto trace_node_names_ind = structural_results.iso_map_to_trace_node_names[iso_map_index];
    auto trace_node_index = structural_results.iso_maps[iso_map_index][node_index];
    auto service_name = structural_results.trace_node_names[trace_node_names_ind][trace_node_index];
    return service_name;
}

std::map<int, std::map<int, std::string>> get_iso_maps_indices_for_which_trace_satifies_curr_condition(
    std::string trace_id, std::string batch_name, std::vector<query_condition>& conditions,
    int curr_cond_ind, fetched_data& evaluation_data, traces_by_structure& structural_results, return_value ret
) {
    std::map<int, std::map<int, std::string>> response;

    auto curr_condition = conditions[curr_cond_ind];
    auto relevant_iso_maps_indices = structural_results.trace_id_to_isomap_indices[trace_id];

    for (auto curr_iso_map_ind : relevant_iso_maps_indices) {
        std::map<int, std::string> node_ind_to_span_id_map;
        bool does_trace_satisfy_condition = false;

        auto condition_service = get_service_name_for_node_index(
            structural_results, curr_iso_map_ind, curr_condition.node_index);

        auto return_service = get_service_name_for_node_index(
            structural_results, curr_iso_map_ind, ret.node_index);

        std::string trace = extract_trace_from_traces_object(trace_id,
            evaluation_data.structural_objects_by_bn[batch_name]);
        std::vector<std::string> trace_lines = split_by_string(trace, newline);

        for (auto line : trace_lines) {
            if (line.find(return_service) != std::string::npos) {
                auto span_info = split_by_string(line, colon);
                auto span_id = span_info[1];
                node_ind_to_span_id_map[ret.node_index] = span_id;
            }

            if (line.find(condition_service) != std::string::npos) {
                auto span_info = split_by_string(line, colon);
                auto span_id = span_info[1];
                auto service_name = span_info[2];
                node_ind_to_span_id_map[curr_condition.node_index] = span_id;
                does_trace_satisfy_condition = does_span_satisfy_condition(
                    span_id, service_name, curr_condition, batch_name, evaluation_data);
            }
        }

        if (true == does_trace_satisfy_condition) {
            response[curr_iso_map_ind] = node_ind_to_span_id_map;
        }
    }

    return response;
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

        if (is_same_hex_str(sp->span_id(), sp->span_id().length(), span_id)) {
            return does_condition_hold(sp, condition);
        }
    }

    return false;
}

