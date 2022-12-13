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

    std::future<StatusOr<traces_by_structure>> struct_filter_obj = std::async(std::launch::async,
        get_traces_by_structure,
        query_trace, start_time, end_time, client);

    time_t earliest_last_updated = -1;
    std::vector<std::future<StatusOr<objname_to_matching_trace_ids>>> index_results_futures;
    for (uint64_t i=0; i < conditions.size(); i++) {
        auto indexed_or_status = is_indexed(&conditions[i], client);
        if (!indexed_or_status.ok()) {
            std::cerr << "Error in is_indexed:" << std::endl;
            std::cerr << indexed_or_status.status().message() << std::endl;
        }
        auto indexed = indexed_or_status.value();
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
        auto res = index_results_futures[i].get();
        if (!res.ok()) {
            std::cerr << "yooo" << std::endl;
            std::cerr << res.status().message() << std::endl;
        } else {
            index_results.push_back(res.value());
        }
        print_progress((i+1.0)/(index_results_futures.size()+1.0), "Retrieving indices", verbose);
    }

    auto struct_results = struct_filter_obj.get();
    if (!struct_results.ok()) {
        std::cerr << "Error in struct_results:" << std::endl;
        std::cerr << struct_results.status().message() << std::endl;
        return {};
    }

    print_progress(1, "Retrieving indices", verbose);
    if (verbose) {
        std::cout << std::endl;
    }

    objname_to_matching_trace_ids intersection = intersect_index_results(
        index_results, struct_results.value(), earliest_last_updated, verbose);

    std::cout << "intersection size is " << intersection.size() << std::endl;

    std::vector<std::future<std::vector<std::string>>> results_futures;
    results_futures.reserve(intersection.size());
    for (auto &map : intersection) {
        results_futures.push_back(std::async(std::launch::async,
                brute_force_per_batch, map.first, map.second, struct_results.value(),
                conditions, ret, query_trace,
                client));
    }

    std::vector<std::string> to_return;
    for (int64_t i = 0; i < results_futures.size(); i++) {
        std::vector<std::string> partial_result = results_futures[i].get();
        to_return.insert(to_return.end(),
                         partial_result.begin(),
                         partial_result.end());
    }

    return to_return;
}

std::vector<std::string> brute_force_per_batch(const std::string batch_name,
                                               std::vector<std::string> trace_ids,
                                               traces_by_structure struct_results,
                                               std::vector<query_condition> conditions,
                                               const return_value ret,
                                               trace_structure query_trace,
                                               gcs::Client* client
                                               ) {
    fetched_data fetched = fetch_data_per_batch(
        struct_results,
        batch_name,
        trace_ids,
        conditions,
        client);

    std::tuple<std::vector<std::string>, std::map<std::string, iso_to_span_id>> filtered;
    if (conditions.size()) {
        filtered = filter_batch_data_based_on_conditions(trace_ids, struct_results, conditions, fetched, ret);
    } else {
        filtered = std::make_tuple(
            trace_ids, get_iso_map_to_span_id_info(struct_results, ret.node_index, client));
    }

    // just a small hack, should check for trace id return instead
    if (ret.type == bytes_value) {
        return std::get<0>(filtered);
    }
    ret_req_data ret_data = fetch_return_data(filtered, ret, fetched, query_trace, batch_name, struct_results, client);
    auto returned = get_return_value(filtered, ret, fetched, query_trace, ret_data, struct_results, client);
    return returned;
}

std::map<std::string, iso_to_span_id> get_iso_map_to_span_id_info(
    traces_by_structure struct_results, int return_node_index, gcs::Client* client) {
    std::map<std::string, iso_to_span_id> res;

    for (auto [k, v] : struct_results.object_name_to_trace_ids_of_interest) {
        auto structural_object_ = read_object(std::string(TRACE_STRUCT_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX), struct_results.object_names[k], client);
        auto structural_object = structural_object_.value();

        for (auto trace_id_index : v) {
            auto trace_id = struct_results.trace_ids[trace_id_index];
            iso_to_span_id res_map;
            auto trace = extract_trace_from_traces_object(trace_id, structural_object);

            for (auto iso_map_index : struct_results.trace_id_to_isomap_indices[trace_id]) {
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
    const std::tuple<std::vector<std::string>, std::map<std::string, iso_to_span_id>> &filtered,
    const return_value &ret, fetched_data &data, trace_structure &query_trace, const std::string batch_name,
    traces_by_structure struct_results,
    gcs::Client* client
) {
    ret_req_data response;

    for (auto const &trace_id : std::get<0>(filtered)) {
        auto relevant_iso_maps_indices = struct_results.trace_id_to_isomap_indices[trace_id];
        for (auto iso_map_index : relevant_iso_maps_indices) {
            std::string service_name = get_service_name_for_node_index(struct_results, iso_map_index, ret.node_index);
            if (
                (data.service_name_to_span_data.find(service_name) ==
                    data.service_name_to_span_data.end())
                &&
                (response.find(service_name) ==
                    response.end())
            ) {
                response[service_name] = read_object_and_parse_traces_data(
                    service_name+BUCKETS_SUFFIX, batch_name, client);
            }
        }
    }
    return response;
}


// Returns index type and last updated
StatusOr<std::tuple<index_type, time_t>> is_indexed(const query_condition *condition, gcs::Client* client) {
    std::string bucket_name = condition->property_name;
    replace_all(bucket_name, ".", "-");
    bucket_name = "index-" + bucket_name + BUCKETS_SUFFIX;
    StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->GetBucketMetadata(bucket_name);
    if (bucket_metadata.status().code() == ::google::cloud::StatusCode::kNotFound ||
        bucket_metadata.status().code() == ::google::cloud::StatusCode::kPermissionDenied) {
        std::tuple<index_type, time_t> res = std::make_pair(none, 0);
        return res;
    }

    if (!bucket_metadata.ok()) {
        std::cout << "in error within is_indexed" << std::endl << std::flush;
        return bucket_metadata.status();
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
        std::tuple<index_type, time_t> res = std::make_pair(bloom, last_indexed);
       return res;
    }
    if (folder_index) {
        std::tuple<index_type, time_t> res = std::make_pair(folder, last_indexed);
        return res;
    }
    std::tuple<index_type, time_t> res = std::make_pair(not_found, 0);
    return res;
}

StatusOr<objname_to_matching_trace_ids> get_traces_by_indexed_condition(
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
            condition->property_name, condition->node_property_value, start_time, end_time, client).value();
        }
        case none: break;
        case not_found: break;
    }
    // TODO(jessberg): Should never get a none or not found, so shouldn't get here.
    objname_to_matching_trace_ids empty;
    return empty;
}

objname_to_matching_trace_ids intersect_index_results(
    std::vector<objname_to_matching_trace_ids> &index_results,
    traces_by_structure &structural_results, const time_t last_updated, const bool verbose) {
    // Easiest solution is just keep a count
    // Eventually we should parallelize this, but I'm not optimizing it
    // until we measure the rest of the code
    // Premature optimization is of the devil and all that.
    print_progress(0, "Intersecting results", verbose);
    std::map<std::tuple<std::string, std::string>, int> count;

    // (1) increment per [batch name, trace ID] for each non-structural index
    for (uint64_t i=0; i < index_results.size(); i++) {
        print_progress(i/index_results.size(), "Intersecting results", verbose);
        for (auto const &obj_to_id : index_results[i]) {
            std::string object = obj_to_id.first;
            for (uint64_t j=0; j < obj_to_id.second.size(); j++) {
                count[std::make_tuple(object, obj_to_id.second[j])] += 1;
            }
        }
    }

    // (2) add 1 for the structural result being correct
    for (auto const &obj_to_id : structural_results.object_name_to_trace_ids_of_interest) {
        std::string obj = structural_results.object_names[obj_to_id.first];
        for (uint64_t j=0; j < obj_to_id.second.size(); j++) {
            count[std::make_tuple(obj, structural_results.trace_ids[obj_to_id.second[j]])] += 1;
        }
    }

    // (3) if we have satisfied all of the indices, include the [batch name, trace ID]
    //     in what we return. otherwise, discard

    int goal_num = index_results.size() + 1;
    objname_to_matching_trace_ids to_return;
    for (auto const &pair : count) {
        auto object = std::get<0>(pair.first);
        if (pair.second == goal_num) {
            auto trace_id = std::get<1>(pair.first);
            to_return[object].push_back(trace_id);
        } else {
            // also include it if the indices haven't caught up to this timestamp
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
    const return_value ret
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
    const std::string bucket_name,
    const std::string object_name, const std::string span_to_find,
    const return_value ret, gcs::Client* client
) {
    std::string contents = read_object(bucket_name, object_name, client).value();
    ot::TracesData trace_data;
    trace_data.ParseFromString(contents);
    return get_return_value_from_traces_data(&trace_data, span_to_find, ret);
}

std::vector<std::string> get_return_value(
    std::tuple<std::vector<std::string>, std::map<std::string, iso_to_span_id>> &filtered,
    const return_value &ret, fetched_data &data, trace_structure &query_trace,
    ret_req_data &return_data, traces_by_structure &struct_results, gcs::Client* client
) {
    std::vector<std::future<std::string>> return_values_fut;
    std::unordered_set<std::string> span_ids;

    for (uint64_t i=0; i < std::get<0>(filtered).size(); i++) {
        std::string trace_id = std::get<0>(filtered)[i];

        // for each trace id, there may be multiple isomaps
        for (auto & ii_ni_sp : std::get<1>(filtered)[trace_id]) {
            std::string span_id_to_find = ii_ni_sp.second[ret.node_index];
            std::string service_name;

            // Now we have the trace and span IDs that define the answer.
            for (std::string& line : split_by_string(data.structural_object, newline)) {
                if (line.find(span_id_to_find) != std::string::npos) {
                    service_name = split_by_string(line, colon)[2];
                }
            }
            if (data.service_name_to_span_data.find(service_name) !=
                data.service_name_to_span_data.end()) {
                ot::TracesData* trace_data =
                    &data.service_name_to_span_data[service_name];
                return_values_fut.push_back(std::async(std::launch::async, get_return_value_from_traces_data,
                    trace_data, span_id_to_find, ret));
            } else if (return_data.find(service_name) !=
                return_data.end()) {
                ot::TracesData* trace_data =
                    &return_data[service_name];
                return_values_fut.push_back(std::async(std::launch::async, get_return_value_from_traces_data,
                    trace_data, span_id_to_find, ret));
            } else {
                std::cout << "wttd " << service_name << " " << std::endl;
                exit(1);
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

// Returns list of trace IDs that match conditions, and trace ID to iso to span ID mapping.
std::tuple<std::vector<std::string>, std::map<std::string, iso_to_span_id>> filter_batch_data_based_on_conditions(
    const std::vector<std::string>& trace_ids,
    traces_by_structure &structural_results,
    std::vector<query_condition> &conditions,
    struct fetched_data &fetched,
    const return_value &ret
) {
    std::vector<std::string> to_return_traces;
    std::map<std::string, iso_to_span_id> trace_id_to_span_id_mappings;

    for (uint64_t i=0; i < trace_ids.size(); i++) {
        // isomap to node to span id
        iso_to_span_id isomap_to_node_to_span_id = does_trace_satisfy_conditions(
            trace_ids[i], conditions, fetched, structural_results, ret);
        if (isomap_to_node_to_span_id.size() > 0) {
            to_return_traces.push_back(trace_ids[i]);
            trace_id_to_span_id_mappings[trace_ids[i]] = isomap_to_node_to_span_id;
        }
    }
    return std::make_tuple(to_return_traces, trace_id_to_span_id_mappings);
}

fetched_data fetch_data_per_batch(
    traces_by_structure& structs_result,
    std::string batch_name,
    const std::vector<std::string> trace_ids,
    std::vector<query_condition> &conditions,
    gcs::Client* client
) {
    fetched_data data;
    if (conditions.size() < 1 || trace_ids.size() < 1) {
        return data;
    }

    std::string trace_structure_bucket_prefix(TRACE_STRUCT_BUCKET_PREFIX);
    std::string buckets_suffix(BUCKETS_SUFFIX);

    data.structural_object = read_object(
                    trace_structure_bucket_prefix+buckets_suffix,
                    batch_name,
                    client).value();

    std::unordered_map<std::string, std::future<ot::TracesData>> data_futures;

    for (auto& trace_id : trace_ids) {
        std::vector<int>& iso_map_indices = structs_result.trace_id_to_isomap_indices[trace_id];
        for (query_condition& curr_condition : conditions) {
            for (int curr_iso_map_ind : iso_map_indices) {
                const int trace_node_names_ind = structs_result.iso_map_to_trace_node_names[curr_iso_map_ind];
                const int trace_node_index = structs_result.iso_maps[curr_iso_map_ind][curr_condition.node_index];
                const std::string& condition_service =
                    structs_result.trace_node_names[trace_node_names_ind][trace_node_index];

                // TODO(jessberg): faster to just get first token?
                const std::string service_name_without_hash_id = split_by_string(condition_service, ":")[0];
                if (data_futures.find(service_name_without_hash_id) ==
                    data_futures.end()
                ) {
                    data_futures[service_name_without_hash_id] = std::async(
                        std::launch::async,
                        read_object_and_parse_traces_data,
                        service_name_without_hash_id+BUCKETS_SUFFIX, batch_name, client);
                }
            }
        }
    }
    for (auto& service_name_to_span_data : data_futures) {
        data.service_name_to_span_data[service_name_to_span_data.first] = service_name_to_span_data.second.get();
    }
    return data;
}

std::map<int, std::map<int, std::string>> does_trace_satisfy_conditions(
    const std::string &trace_id,
    std::vector<query_condition> &conditions, fetched_data& evaluation_data,
    traces_by_structure &structural_results, const return_value& ret
) {
    // isomap_index_to_node_index_to_span_id -> ii_to_ni_to_si
    std::vector<std::map<int, std::map<int, std::string>>> ii_to_ni_to_si_data_for_all_conditions;
    for (uint64_t curr_cond_ind = 0; curr_cond_ind < conditions.size(); curr_cond_ind++) {
        ii_to_ni_to_si_data_for_all_conditions.push_back(
            get_iso_maps_indices_for_which_trace_satifies_curr_condition(
                trace_id, conditions, curr_cond_ind, evaluation_data, structural_results, ret));
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
    const std::string &trace_id,
    std::vector<query_condition>& conditions,
    int curr_cond_ind, fetched_data& evaluation_data,
    traces_by_structure& structural_results, const return_value& ret
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
            evaluation_data.structural_object);

        for (auto line : split_by_string(trace, newline)) {
            if (line.find(return_service) != std::string::npos) {
                node_ind_to_span_id_map[ret.node_index] = split_by_string(line, colon)[1];
            }

            if (line.find(condition_service) != std::string::npos) {
                auto span_info = split_by_string(line, colon);
                node_ind_to_span_id_map[curr_condition.node_index] = span_info[1];
                does_trace_satisfy_condition = does_trace_satisfy_condition || does_span_satisfy_condition(
                    span_info[1], span_info[2], curr_condition, evaluation_data);
            }
        }

        if (true == does_trace_satisfy_condition) {
            response[curr_iso_map_ind] = node_ind_to_span_id_map;
        }
    }

    return response;
}

bool does_span_satisfy_condition(
    const std::string &span_id, const std::string &service_name,
    const query_condition &condition, fetched_data& evaluation_data
) {
    ot::TracesData* trace_data = &(evaluation_data.service_name_to_span_data[service_name]);

    const ot::Span* sp;
    for (int i=0; i < trace_data->resource_spans(0).scope_spans(0).spans_size(); i++) {
        sp = &(trace_data->resource_spans(0).scope_spans(0).spans(i));

        if (is_same_hex_str(sp->span_id(), span_id)) {
            auto res = does_condition_hold(sp, condition);
            return res;
        }
    }
    return false;
}

