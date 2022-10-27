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

    time_t earliest_last_updated = -1;
    std::vector<std::future<objname_to_matching_trace_ids>> index_results_futures;
    for (uint64_t i=0; i < conditions.size(); i++) {
        auto indexed = is_indexed(&conditions[i], client);
        index_type i_type = std::get<0>(indexed);
        if (i_type != none) {
            if (earliest_last_updated == -1 || std::get<1>(indexed) < earliest_last_updated) {
                earliest_last_updated = std::get<1>(indexed);
            }
            index_results_futures.push_back(std::async(std::launch::async, get_traces_by_indexed_condition,
            start_time, end_time, &conditions[i], i_type, client));
        }
    }
    print_progress(0, "Retrieving indices", verbose);

    std::vector<objname_to_matching_trace_ids> index_results;
    index_results.reserve(index_results_futures.size());
    for (uint64_t i=0; i < index_results_futures.size(); i++) {
        index_results.push_back(index_results_futures[i].get());
        print_progress((i+1.0)/(index_results_futures.size()+1.0), "Retrieving indices", verbose);
    }
    auto struct_results = struct_filter_obj.get();
    print_progress(1, "Retrieving indices", verbose);
    if (verbose) {
        std::cout << std::endl;
    }

    if (verbose) {
        std::cout << "Index Results: " << index_results.size() << std::endl;
        std::cout << "Struct Results: " << struct_results.trace_ids.size() << std::endl;
    }

    if (conditions.size()) {
        auto intersection = intersect_index_results(
            index_results, struct_results, earliest_last_updated, verbose);
        
        fetched_data fetched = fetch_data(
            struct_results,
            intersection,
            conditions,
            client);

        if (verbose) {
            std::cout << "fetched data" << std::endl;
            std::cout << "Intersectipn size: " << intersection.size() << std::endl;
        }

        auto filtered = filter_based_on_conditions(
            intersection, struct_results, conditions, fetched, ret);

        if (verbose) {
            std::cout << "Filtered size: " << std::get<0>(filtered).size() << std::endl;
            std::cout << "filtered based on conditions" << std::endl;
        }

        ret_req_data spans_objects_by_bn_sn = fetch_return_data(filtered, ret, fetched, query_trace, client);
        auto returned = get_return_value(filtered, ret, fetched, query_trace, spans_objects_by_bn_sn, client);
        return returned;
    } else {
        objname_to_matching_trace_ids morphed_struct_results = morph_struct_result_to_objname_to_matching_trace_ids(struct_results);
        std::tuple<objname_to_matching_trace_ids, std::map<std::string, iso_to_span_id>> current_result (std::make_tuple(
            morphed_struct_results,
            get_iso_map_to_span_id_info(struct_results, ret.node_index, client)));
        fetched_data fetched;
        ret_req_data spans_objects_by_bn_sn = fetch_return_data(current_result, ret, fetched, query_trace, client);
        auto returned = get_return_value(current_result, ret, fetched, query_trace, spans_objects_by_bn_sn, client);
        return returned;
    }
}

std::map<std::string, iso_to_span_id> get_iso_map_to_span_id_info(traces_by_structure struct_results, int return_node_index, gcs::Client* client) {
    std::map<std::string, iso_to_span_id> res;

    for (auto [k, v] : struct_results.object_name_to_trace_ids_of_interest) {
        auto structural_object = read_object(TRACE_STRUCT_BUCKET, struct_results.object_names[k], client);
        
        for (auto trace_id_index: v) {
            auto trace_id = struct_results.trace_ids[trace_id_index];
            iso_to_span_id res_map;
            auto trace = extract_trace_from_traces_object(trace_id, structural_object);

            for (auto iso_map_index : struct_results.trace_id_to_isomap_indices[trace_id] ) {
                std::map<int, std::string> node_ind_to_span_id_map;
                auto return_service = get_service_name_for_node_index(struct_results, iso_map_index, return_node_index);
                
                for (auto line : split_by_string(trace, newline)) {
                    if (line.find(return_service) != std::string::npos) {
                        node_ind_to_span_id_map[return_node_index] = split_by_string(line, colon)[1];
                    }
                }

                res_map[iso_map_index] = node_ind_to_span_id_map;
            }

            res[trace_id] = res_map;
        }
    }
    return res;
}

objname_to_matching_trace_ids morph_struct_result_to_objname_to_matching_trace_ids(traces_by_structure struct_results) {
    objname_to_matching_trace_ids res;
    for (auto [k, v] : struct_results.object_name_to_trace_ids_of_interest) {
        std::vector<std::string> trace_ids;
        for (auto trace_id_ind : v) {
            trace_ids.push_back(struct_results.trace_ids[trace_id_ind]);
        }
        res[struct_results.object_names[k]] = trace_ids;
    }
    return res;
}

ret_req_data fetch_return_data(
    const std::tuple<objname_to_matching_trace_ids, std::map<std::string, iso_to_span_id>> &filtered,
    return_value &ret, fetched_data &data, trace_structure &query_trace, gcs::Client* client
) {
    ret_req_data response;

    for (auto const &obj_to_trace_ids : std::get<0>(filtered)) {
        std::string object = obj_to_trace_ids.first;
        std::string service_name = query_trace.node_names[ret.node_index];
        if (
            (data.spans_objects_by_bn_sn[object].find(service_name) ==
                data.spans_objects_by_bn_sn[object].end())
            &&
            (response[object].find(service_name) ==
                response[object].end())
        ) {
            response[object][service_name] = read_object_and_parse_traces_data(
                service_name+BUCKETS_SUFFIX, object, client);
        }
    }
    return response;
}


// Returns index type and last updated
std::tuple<index_type, time_t> is_indexed(const query_condition *condition, gcs::Client* client) {
    std::string bucket_name = condition->property_name;
    replace_all(bucket_name, ".", "-");
    StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->GetBucketMetadata(bucket_name);
    if (bucket_metadata.status().code() == ::google::cloud::StatusCode::kNotFound) {
        return std::make_pair(none, 0);
    }
    if (!bucket_metadata) {
        std::cout << "in error within is_indexed" << std::endl << std::flush;
        throw std::runtime_error(bucket_metadata.status().message());
    }
    bool bloom_index = false;
    bool folder_index = false;
    time_t last_indexed = 0;
    for (auto const& kv : bucket_metadata->labels()) {
        if (kv.first == "bucket_type") {
            if (kv.second == "bloom_index") {
                bloom_index = true;
            } else if (kv.first == "folder_index") {
                folder_index = true;
            }
        }
        if (kv.first == "last_indexed")  {
            last_indexed = std::stoi(kv.second);
        }
        if (kv.first == "root") {
            last_indexed = std::stoi(split_by_string(kv.second, hyphen)[0]);
        }
    }
    if (bloom_index) {
       return std::make_pair(bloom, last_indexed);
    }
    if (folder_index) {
        return std::make_pair(folder, last_indexed);
    }
    return std::make_pair(not_found, 0);
}

objname_to_matching_trace_ids get_traces_by_indexed_condition(
    const int start_time, const int end_time, const query_condition *condition, const index_type ind_type,
    gcs::Client* client) {
    switch (ind_type) {
        case bloom: {
            assert(condition->comp == Equal_to);
            std::string bucket_name = condition->property_name;
            replace_all(bucket_name, ".", "-");
            return query_bloom_index_for_value(client, condition->node_property_value, bucket_name,
            start_time, end_time);
        }
        case folder: {
            return get_obj_name_to_trace_ids_map_from_folders_index(
            condition->property_name, condition->node_property_value, start_time, end_time, client);
        }
        case none: break;
        case not_found: break;
    }
    // TODO(jessberg): Should never get a none or not found, so shouldn't get here.
    objname_to_matching_trace_ids empty;
    return empty;
}

std::tuple<objname_to_matching_trace_ids, std::map<std::string, iso_to_span_id>> filter_based_on_conditions(
    objname_to_matching_trace_ids &intersection,
    traces_by_structure &structural_results,
    std::vector<query_condition> &conditions,
    struct fetched_data &fetched,
    return_value &ret
) {
    std::vector<
        std::future<
            std::tuple<objname_to_matching_trace_ids, std::map<std::string, iso_to_span_id>>>> response_futures;

    for (const auto &object_to_trace : intersection) {
        response_futures.push_back(std::async(std::launch::async,
            filter_based_on_conditions_batched, std::ref(intersection), object_to_trace.first,
            std::ref(structural_results), std::ref(conditions), std::ref(fetched), ret));
    }

    objname_to_matching_trace_ids obj_to_trace_ids;
    std::map<std::string, iso_to_span_id> trace_id_to_maps;

    for_each(response_futures.begin(), response_futures.end(),
		[&obj_to_trace_ids, &trace_id_to_maps](
            std::future<std::tuple<objname_to_matching_trace_ids, std::map<std::string, iso_to_span_id>>>& fut) {
			    std::tuple<
                    objname_to_matching_trace_ids,
                    std::map<std::string, iso_to_span_id>> response_tuple = fut.get();

                obj_to_trace_ids.insert(std::get<0>(response_tuple).begin(), std::get<0>(response_tuple).end());
                trace_id_to_maps.insert(std::get<1>(response_tuple).begin(), std::get<1>(response_tuple).end());
	});

    return {obj_to_trace_ids, trace_id_to_maps};
}

std::tuple<objname_to_matching_trace_ids, std::map<std::string, iso_to_span_id>> filter_based_on_conditions_batched(
    objname_to_matching_trace_ids &intersection,
    std::string object_name_to_process,
    traces_by_structure &structural_results,
    std::vector<query_condition> &conditions,
    struct fetched_data &fetched,
    return_value ret
) {
    objname_to_matching_trace_ids to_return_traces;
    std::map<std::string, iso_to_span_id> trace_id_to_span_id_mappings;

    auto* trace_ids = &(intersection[object_name_to_process]);
    for (uint64_t i=0; i < trace_ids->size(); i++) {
        iso_to_span_id res_ii_to_ni_to_si = does_trace_satisfy_conditions(
            (*trace_ids)[i], object_name_to_process, conditions, fetched, structural_results, ret);
        if (res_ii_to_ni_to_si.size() > 0) {
            to_return_traces[object_name_to_process].push_back((*trace_ids)[i]);
            trace_id_to_span_id_mappings[(*trace_ids)[i]] = res_ii_to_ni_to_si;
        }
    }
    return std::make_tuple(to_return_traces, trace_id_to_span_id_mappings);
}

objname_to_matching_trace_ids intersect_index_results(
    std::vector<objname_to_matching_trace_ids> &index_results,
    traces_by_structure &structural_results, time_t last_updated, bool verbose) {
    // Easiest solution is just keep a count
    // Eventually we should parallelize this, but I'm not optimizing it
    // until we measure the rest of the code
    // Premature optimization is of the devil and all that.
    print_progress(0, "Intersecting results", verbose);
    std::map<std::tuple<std::string, std::string>, int> count;
    for (uint64_t i=0; i < index_results.size(); i++) {
        print_progress(i/index_results.size(), "Intersecting results", verbose);
        for (auto const &obj_to_id : index_results[i]) {
            std::string object = obj_to_id.first;
            for (uint64_t j=0; j < obj_to_id.second.size(); j++) {
                count[std::make_tuple(object, obj_to_id.second[j])] += 1;
            }
        }
    }

    std::map<int, std::string> ind_to_trace_id;
    std::map<int, std::string> ind_to_obj;
    for (uint64_t i=0; i < structural_results.trace_ids.size(); i++) {
        ind_to_trace_id[i] = structural_results.trace_ids[i];
    }
    for (uint64_t i=0; i < structural_results.object_names.size(); i++) {
        ind_to_obj[i] = structural_results.object_names[i];
    }
    for (auto const &obj_to_id : structural_results.object_name_to_trace_ids_of_interest) {
        std::string obj = ind_to_obj[obj_to_id.first];
        for (uint64_t j=0; j < obj_to_id.second.size(); j++) {
            count[std::make_tuple(obj, ind_to_trace_id[obj_to_id.second[j]])] += 1;
        }
    }

    int goal_num = index_results.size() + 1;
    objname_to_matching_trace_ids to_return;
    for (auto const &pair : count) {
        auto object = std::get<0>(pair.first);
        if (pair.second == goal_num) {
            auto trace_id = std::get<1>(pair.first);
            to_return[object].push_back(trace_id);
        } else {
            auto timestamps = extract_batch_timestamps(object);
            if (std::get<0>(timestamps) > last_updated) {
                auto trace_id = std::get<1>(pair.first);
                to_return[object].push_back(trace_id);
            }
        }
    }
    print_progress(1, "Intersecting results", verbose);
    if (verbose) {
        std::cout << std::endl;
    }
    return to_return;
}

std::string get_return_value_from_traces_data(
    ot::TracesData *trace_data,
    const std::string span_to_find,
    return_value ret
) {
     int sp_size = trace_data->resource_spans(0).scope_spans(0).spans_size();
     for (int i=0; i < sp_size; i++) {
        const ot::Span *sp =
            &trace_data->resource_spans(0).scope_spans(0).spans(i);
        if (is_same_hex_str(sp->ot::Span::span_id(), span_to_find)) {
            return get_value_as_string(sp, ret.func, ret.type);
        }
    }
    std::cerr << "didn't find the span " << span_to_find << " I was looking for " << std::endl << std::flush;
    return "";
}

std::string retrieve_object_and_get_return_value_from_traces_data(
    std::string bucket_name,
    std::string object_name, const std::string span_to_find,
    return_value ret, gcs::Client* client
) {
    std::string contents = read_object(bucket_name, object_name, client);
    ot::TracesData trace_data;
    trace_data.ParseFromString(contents);
    return get_return_value_from_traces_data(&trace_data, span_to_find, ret);
}

std::vector<std::string> get_return_value(
    std::tuple<objname_to_matching_trace_ids, std::map<std::string, iso_to_span_id>> &filtered,
    return_value &ret, fetched_data &data, trace_structure &query_trace,
    ret_req_data &spans_objects_by_bn_sn, gcs::Client* client
) {
    std::vector<std::future<std::string>> return_values_fut;
    std::unordered_set<std::string> span_ids;

    for (auto const &obj_to_trace_ids : std::get<0>(filtered)) {
        std::string object = obj_to_trace_ids.first;
        for (uint64_t i=0; i < obj_to_trace_ids.second.size(); i++) {
            std::string trace_id = obj_to_trace_ids.second[i];
            
            // for each trace id, there may be multiple isomaps
            for (auto & ii_ni_sp : std::get<1>(filtered)[trace_id]) {
                std::string span_id_to_find = ii_ni_sp.second[ret.node_index];
                if (span_ids.find(span_id_to_find) != span_ids.end()) {
                    continue;
                } else {
                    span_ids.insert(span_id_to_find);
                }
                std::string service_name = query_trace.node_names[ret.node_index];

                if (data.spans_objects_by_bn_sn[object].find(service_name) !=
                    data.spans_objects_by_bn_sn[object].end()) {
                    ot::TracesData* trace_data =
                        &data.spans_objects_by_bn_sn[object][service_name];
                    return_values_fut.push_back(std::async(std::launch::async, get_return_value_from_traces_data,
                        trace_data, span_id_to_find, ret));
                } else if (spans_objects_by_bn_sn[object].find(service_name) !=
                    spans_objects_by_bn_sn[object].end()) {
                    ot::TracesData* trace_data =
                        &spans_objects_by_bn_sn[object][service_name];
                    return_values_fut.push_back(std::async(std::launch::async, get_return_value_from_traces_data,
                        trace_data, span_id_to_find, ret));
                } else {
                    std::cout << "wttd " << service_name << " " << object << std::endl;
                    exit(1);
                }
            }
        }
    }
    std::vector<std::string> to_return;
    to_return.reserve(return_values_fut.size());
    for (uint64_t i=0; i < return_values_fut.size(); i++) {
        to_return.push_back(return_values_fut[i].get());
    }

    return to_return;
}

/**
 * Fetches data that is required for evaluating conditions. 
 */
fetched_data fetch_data(
    traces_by_structure& structs_result,
    objname_to_matching_trace_ids& object_name_to_trace_ids_of_interest,
    std::vector<query_condition> &conditions,
    gcs::Client* client
) {
    fetched_data response;

    std::unordered_map<
        std::string,
        std::unordered_map<
            std::string,
            std::future<
                ot::TracesData>>> response_futures;

    std::string trace_structure_bucket_prefix(TRACE_STRUCT_BUCKET_PREFIX);
    std::string buckets_suffix(BUCKETS_SUFFIX);

    for (auto& trace_id_map : object_name_to_trace_ids_of_interest) {
        const std::string& batch_name = trace_id_map.first;
        const std::vector<std::string>& trace_ids = trace_id_map.second;
        if (trace_ids.size() < 1) {
            continue;
        }

        if (response.structural_objects_by_bn.find(batch_name) == response.structural_objects_by_bn.end()) {
            response.structural_objects_by_bn[batch_name] = read_object(
                trace_structure_bucket_prefix+buckets_suffix, batch_name, client);
        }

        std::vector<int>& iso_map_indices = structs_result.trace_id_to_isomap_indices[trace_ids[0]];
        for (query_condition& curr_condition : conditions) {
            for (int curr_iso_map_ind : iso_map_indices) {
                const int trace_node_names_ind = structs_result.iso_map_to_trace_node_names[curr_iso_map_ind];
                const int trace_node_index = structs_result.iso_maps[curr_iso_map_ind][curr_condition.node_index];
                const std::string& condition_service =
                    structs_result.trace_node_names[trace_node_names_ind][trace_node_index];

                /**
                 * @brief while parallelizing, just make 
                 * response.spans_objects_by_bn_sn[batch_name][service_name_without_hash_id] = true
                 * sort of map first and then make asynchronous calls on em. cuz there can be duplicate calls to
                 * spans_objects_by_bn_sn[batch_name][service_name_without_hash_id], so we dont wanna fetch same obj
                 * more than once.
                 */
                const std::string service_name_without_hash_id = split_by_string(condition_service, ":")[0];
                if (response_futures[batch_name].find(service_name_without_hash_id) ==
                    response_futures[batch_name].end()
                ) {
                    response_futures[batch_name][service_name_without_hash_id] = std::async(
                        std::launch::async,
                        read_object_and_parse_traces_data,
                        service_name_without_hash_id+BUCKETS_SUFFIX, batch_name, client);
                }
            }
        }
    }

    for (auto& first_kv : response_futures) {
        auto bn = first_kv.first;
        for (auto& second_kv : first_kv.second) {
            auto sn = second_kv.first;
            response.spans_objects_by_bn_sn[bn][sn] = second_kv.second.get();
        }
    }

    return response;
}

std::map<int, std::map<int, std::string>> does_trace_satisfy_conditions(
    const std::string &trace_id, const std::string &object_name,
    std::vector<query_condition> &conditions, fetched_data& evaluation_data,
    traces_by_structure &structural_results, return_value& ret
) {
    // isomap_index_to_node_index_to_span_id -> ii_to_ni_to_si
    std::vector<std::map<int, std::map<int, std::string>>> ii_to_ni_to_si_data_for_all_conditions;
    for (uint64_t curr_cond_ind = 0; curr_cond_ind < conditions.size(); curr_cond_ind++) {
        ii_to_ni_to_si_data_for_all_conditions.push_back(
            get_iso_maps_indices_for_which_trace_satifies_curr_condition(
                trace_id, object_name, conditions, curr_cond_ind, evaluation_data, structural_results, ret));
    }

    std::map<int, std::map<int, std::string>> aggregate_result;
    std::map<int, uint64_t> iso_map_to_satisfied_conditions_map;
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
    const std::string &trace_id, const std::string &batch_name,
    std::vector<query_condition>& conditions,
    int curr_cond_ind, fetched_data& evaluation_data, traces_by_structure& structural_results, return_value& ret
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

        for (auto line : split_by_string(trace, newline)) {
            if (line.find(return_service) != std::string::npos) {
                node_ind_to_span_id_map[ret.node_index] = split_by_string(line, colon)[1];
            }

            if (line.find(condition_service) != std::string::npos) {
                auto span_info = split_by_string(line, colon);
                node_ind_to_span_id_map[curr_condition.node_index] = span_info[1];
                does_trace_satisfy_condition = does_span_satisfy_condition(
                    span_info[1], span_info[2], curr_condition, batch_name, evaluation_data);
            }
        }

        if (true == does_trace_satisfy_condition) {
            response[curr_iso_map_ind] = node_ind_to_span_id_map;
        }
    }

    return response;
}

bool does_span_satisfy_condition(
    std::string &span_id, std::string &service_name,
    query_condition &condition, const std::string &batch_name, fetched_data& evaluation_data
) {
    if (evaluation_data.spans_objects_by_bn_sn.find(batch_name) == evaluation_data.spans_objects_by_bn_sn.end()
    || evaluation_data.spans_objects_by_bn_sn[batch_name].find(
        service_name) == evaluation_data.spans_objects_by_bn_sn[batch_name].end()) {
            std::cerr << "Error in does_span_satisfy_condition: Required data not found!" << std::endl;
            exit(1);
    }

    ot::TracesData* trace_data = &(evaluation_data.spans_objects_by_bn_sn[batch_name][service_name]);

    const ot::Span* sp;
    for (int i=0; i < trace_data->resource_spans(0).scope_spans(0).spans_size(); i++) {
        sp = &(trace_data->resource_spans(0).scope_spans(0).spans(i));

        if (is_same_hex_str(sp->span_id(), span_id)) {
            return does_condition_hold(sp, condition);
        }
    }

    return false;
}

