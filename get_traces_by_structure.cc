#include "get_traces_by_structure.h"

StatusOr<traces_by_structure> get_traces_by_structure(
    trace_structure query_trace, int start_time, int end_time, gcs::Client* client) {

    std::string prefix_to_search = std::string(TRACE_HASHES_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX);

    std::vector<std::future<StatusOr<potential_prefix_struct>>> future_potential_prefixes;
    for (auto&& prefix : client->ListObjectsAndPrefixes(prefix_to_search, gcs::Delimiter("/"))) {
        if (!prefix) {
            std::cerr << "Error in getting prefixes" << std::endl;
            return prefix.status();
        }

        auto result = *std::move(prefix);
        if (false == absl::holds_alternative<std::string>(result)) {
            std::cerr << "Error in getting prefixes" << std::endl;
            return Status(
                google::cloud::StatusCode::kUnavailable, "error while moving prefix in get_traces_by_structure");
        }

        // Get mapping from batch name to prefix and trace ID.
        future_potential_prefixes.push_back(std::async(std::launch::async,
            get_potential_prefixes, absl::get<std::string>(result), client));
    }

    // Now map from batch name to prefix and trace ID, so you can check
    // exemplar validity using the same data.
    std::unordered_map<std::string, std::vector<std::pair<std::string, std::string>>> batch_name_map;
    for (int64_t i=0; i < future_potential_prefixes.size(); i++) {
        StatusOr<potential_prefix_struct> p = future_potential_prefixes[i].get();
        if (!p.ok()) { std::cerr << "can't get prefixes" << std::endl; return p.status(); }
        batch_name_map[p->batch_name].push_back(std::make_pair(p->prefix, p->trace_id));
    }

    std::vector<std::string> all_object_names = get_batches_between_timestamps(client, start_time, end_time);
    std::vector<std::future<StatusOr<traces_by_structure>>> response_futures;

    for (auto& [batch_name, prefix_and_trace_id] : batch_name_map) {
        response_futures.push_back(std::async(std::launch::async,
            filter_by_query, batch_name, prefix_and_trace_id,
            query_trace, start_time, end_time, all_object_names, client));
    }

    traces_by_structure to_return;

    for (int64_t i=0; i < response_futures.size(); i++) {
        StatusOr<traces_by_structure> values_to_return = response_futures[i].get();
        if (!values_to_return.ok()) {
            std::cerr << "so sad for you, response futures are bad" << std::endl;
            return values_to_return.status();
        }
        merge_traces_by_struct(values_to_return.value(), &to_return);
    }


    return to_return;
}

void merge_traces_by_struct(const traces_by_structure &new_trace_by_struct, traces_by_structure* old ) {
    int trace_id_offset = old->trace_ids.size();
    int iso_map_offset = old->iso_maps.size();

    // first merge the vectors of the data itself
    old->trace_ids.insert(old->trace_ids.end(),
                                new_trace_by_struct.trace_ids.begin(),
                                new_trace_by_struct.trace_ids.end());
    old->object_names.insert(old->object_names.end(),
                                    new_trace_by_struct.object_names.begin(),
                                    new_trace_by_struct.object_names.end());
    old->iso_maps.insert(old->iso_maps.end(),
                                new_trace_by_struct.iso_maps.begin(),
                                new_trace_by_struct.iso_maps.end());
    old->trace_node_names.insert(old->trace_node_names.end(),
                                        new_trace_by_struct.trace_node_names.begin(),
                                        new_trace_by_struct.trace_node_names.end());

    // now merge the pointers by adding the offsets to everything
    for (const auto &pair : new_trace_by_struct.object_name_to_trace_ids_of_interest) {
        auto object_name = pair.first;
        for (uint64_t i=0; i < pair.second.size(); i++) {
            old->object_name_to_trace_ids_of_interest[object_name].push_back(
                pair.second[i] + trace_id_offset);
        }
    }

    for (const auto &pair : new_trace_by_struct.trace_id_to_isomap_indices) {
        std::vector<int> isomap_indices;
        for (uint64_t i=0; i < pair.second.size(); i++) {
            isomap_indices.push_back(pair.second[i] + iso_map_offset);
        }
        old->trace_id_to_isomap_indices[pair.first] = isomap_indices;
    }

    // then finally deal with trace node name stuff
    if (new_trace_by_struct.trace_node_names.size() > 0) {
        old->trace_node_names.push_back(new_trace_by_struct.trace_node_names[0]);
        int tnn_index = old->trace_node_names.size()-1;
        for (uint64_t i=iso_map_offset; i < old->iso_maps.size(); i++) {
            old->iso_map_to_trace_node_names[i] = tnn_index;
        }
    }
}

StatusOr<std::string> get_examplar_from_prefix(std::string prefix, gcs::Client* client) {
    std::string prefix_to_search = std::string(TRACE_HASHES_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX);
    std::string object_name = "";

    for (auto&& object_metadata : client->ListObjects(prefix_to_search, gcs::Prefix(prefix), gcs::MaxResults(1))) {
        if (!object_metadata) {
            std::cerr << object_metadata.status().message() << std::endl;
            return object_metadata.status();
        }
        object_name =  object_metadata->name();

        // MaxResults param just limits the size of the page, we do not need
        // to iterate over more data, so break...
        break;
    }

    auto response_trace_ids_or_status = get_trace_ids_from_trace_hashes_object(object_name, client);
    if (!response_trace_ids_or_status.ok()) {
        return response_trace_ids_or_status.status();
    }

    auto response_trace_ids = response_trace_ids_or_status.value();

    std::string batch_name = extract_batch_name(object_name);
    auto object_content_or_status = read_object(TRACE_STRUCT_BUCKET_PREFIX+std::string(BUCKETS_SUFFIX),
        batch_name, client);
    if (!object_content_or_status.ok()) {
        return Status(google::cloud::StatusCode::kUnavailable, "could not get examplar");
    }

    auto object_content = object_content_or_status.value();
    std::string trace = extract_any_trace(response_trace_ids, object_content);
    return trace;
}

/**
 * Checks examplar validity and sets isomaps and node_names in the passed by reference param to_return.
*/
StatusOr<bool> check_examplar_validity(
    std::string examplar, trace_structure query_trace, traces_by_structure& to_return) {
    trace_structure candidate_trace = morph_trace_object_to_trace_structure(examplar);

    auto iso_mappings = get_isomorphism_mappings(candidate_trace, query_trace);
    if (iso_mappings.size() < 1) {
        return false;
    }

    // Store Isomaps
    to_return.iso_maps = iso_mappings;

    // Store Node Names
    std::vector<std::unordered_map<int, std::string>> nn;
    nn.push_back(candidate_trace.node_names);
    to_return.trace_node_names = nn;

    return true;
}

Status get_traces_by_structure_data(
    gcs::Client* client,
    std::string prefix,
    std::string batch_name,
    std::string root_service_name,
    time_t start_time, time_t end_time,
    traces_by_structure& to_return
) {
    auto hashes_bucket_object_name = prefix + batch_name;

    if (false == is_object_within_timespan(extract_batch_timestamps(batch_name), start_time, end_time)) {
        return Status();
    }

    auto response_trace_ids_or_status = get_trace_ids_from_trace_hashes_object(hashes_bucket_object_name, client);

    if (!response_trace_ids_or_status.ok()) {
        return Status(
            google::cloud::StatusCode::kUnavailable,
            "error in get_traces_by_structure_data: get_trace_ids_from_trace_hashes_object");
    }

    auto response_trace_ids = response_trace_ids_or_status.value();
    if (response_trace_ids.size() < 1) {
        return Status();
    }

    auto trace_ids_to_append = response_trace_ids;

    if (object_could_have_out_of_bound_traces(extract_batch_timestamps(batch_name), start_time, end_time)) {
        auto trace_ids_to_append_or_status = filter_trace_ids_based_on_query_timestamp_for_given_root_service(
            response_trace_ids, batch_name, start_time, end_time, root_service_name, client);
        if (!trace_ids_to_append_or_status.ok()) {
            return trace_ids_to_append_or_status.status();
        }
        trace_ids_to_append = trace_ids_to_append_or_status.value();
    }

    int trace_id_offset = to_return.trace_ids.size();
    to_return.trace_ids.insert(to_return.trace_ids.end(),
        trace_ids_to_append.begin(), trace_ids_to_append.end());

    to_return.object_names.push_back(batch_name);
    int batch_name_index = to_return.object_names.size()-1;
    for (uint64_t i=trace_id_offset; i < to_return.trace_ids.size(); i++) {
        for (uint64_t j=0; j < to_return.iso_maps.size(); j++) {
            to_return.trace_id_to_isomap_indices[to_return.trace_ids[i]].push_back(j);
        }
        to_return.object_name_to_trace_ids_of_interest[batch_name_index].push_back(i);
    }

    return Status();
}

StatusOr<potential_prefix_struct> get_potential_prefixes(
    std::string prefix, gcs::Client* client) {
    std::string prefix_to_search = std::string(TRACE_HASHES_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX);
    std::string object_name = "";

    for (auto&& object_metadata : client->ListObjects(prefix_to_search, gcs::Prefix(prefix), gcs::MaxResults(1))) {
        if (!object_metadata) {
            std::cerr << object_metadata.status().message() << std::endl;
            return object_metadata.status();
        }
        object_name =  object_metadata->name();

        // MaxResults param just limits the size of the page, we do not need
        // to iterate over more data, so break...
        break;
    }

    auto response_trace_ids_or_status = get_trace_ids_from_trace_hashes_object(object_name, client);
    if (!response_trace_ids_or_status.ok()) {
        return response_trace_ids_or_status.status();
    }

    auto response_trace_ids = response_trace_ids_or_status.value();

    std::string batch_name = extract_batch_name(object_name);
    return potential_prefix_struct {
        .prefix = prefix,
        .batch_name = batch_name,
        .trace_id = response_trace_ids[0],
    };
}

StatusOr<traces_by_structure> filter_by_query(std::string batch_name,
    std::vector<std::pair<std::string, std::string>> prefix_to_trace_ids,
    trace_structure query_trace, int start_time, int end_time,
    const std::vector<std::string>& all_object_names, gcs::Client* client) {
    traces_by_structure to_return;
    auto object_content_or_status = read_object(TRACE_STRUCT_BUCKET_PREFIX+std::string(BUCKETS_SUFFIX),
        batch_name, client);
    if (!object_content_or_status.ok()) {
        return Status(google::cloud::StatusCode::kUnavailable, "could not get examplar");
    }

    auto object_content = object_content_or_status.value();
    for (int64_t i=0; i < prefix_to_trace_ids.size(); i++) {
        traces_by_structure cur_traces_by_structure;
        std::vector<std::string> trace_ids;
        trace_ids.push_back(std::get<1>(prefix_to_trace_ids[i]));
        std::string trace = extract_any_trace(trace_ids, object_content);
        auto valid = check_examplar_validity(trace, query_trace, cur_traces_by_structure);
        if (!valid.ok()) {
            return valid.status();
        }

        if (!valid.value()) {
            continue;
        }

        auto root_service_name = get_root_service_name(trace);
        for (auto batch_name : all_object_names) {
            auto status = get_traces_by_structure_data(
                client, std::get<0>(prefix_to_trace_ids[i]), batch_name,
                root_service_name, start_time, end_time, cur_traces_by_structure);

            if (!status.ok()) {
                return status;
            }
        }
        merge_traces_by_struct(cur_traces_by_structure, &to_return);
    }
    return to_return;
}

StatusOr<traces_by_structure> process_trace_hashes_prefix_and_retrieve_relevant_trace_ids(
    std::string prefix, trace_structure query_trace, int start_time, int end_time,
    const std::vector<std::string>& all_object_names, gcs::Client* client
) {
    traces_by_structure to_return;

    auto examplar_trace = get_examplar_from_prefix(prefix, client);
    if (!examplar_trace.ok()) {
        return examplar_trace.status();
    }

    auto valid = check_examplar_validity(examplar_trace.value(), query_trace, to_return);
    if (!valid.ok()) {
        return valid.status();
    }

    if (!valid.value()) {
        return to_return;
    }

    auto root_service_name = get_root_service_name(examplar_trace.value());
    for (auto batch_name : all_object_names) {
        auto status = get_traces_by_structure_data(
            client, prefix, batch_name, root_service_name, start_time, end_time, to_return);

        if (!status.ok()) {
            return status;
        }
    }

    return to_return;
}

std::string get_root_service_name(const std::string &trace) {
    for (const std::string& line : split_by_string(trace, newline)) {
        if (line.substr(0, 1) == ":") {
            return split_by_string(line, colon)[2];
        }
    }
    return "";
}

StatusOr<std::vector<std::string>> filter_trace_ids_based_on_query_timestamp_for_given_root_service(
    std::vector<std::string> &trace_ids,
    std::string &batch_name,
    const int start_time,
    const int end_time,
    std::string &root_service_name,
    gcs::Client* client) {
    std::vector<std::string> response;

    auto spans_data = read_object(root_service_name + std::string(BUCKETS_SUFFIX), batch_name, client);
    if (!spans_data.ok()) {
        return spans_data.status();
    }

    std::map<std::string, std::pair<int, int>> trace_id_to_timestamp_map = get_timestamp_map_for_trace_ids(
        spans_data.value(), trace_ids);

    for (const auto& trace_id : trace_ids) {
        std::pair<int, int> trace_timestamp = trace_id_to_timestamp_map[trace_id];
        if (is_object_within_timespan(trace_timestamp, start_time, end_time)) {
            response.push_back(trace_id);
        }
    }

    return response;
}

/**
 * @brief Get the isomorphism mappings object
 *
 * Map: query trace => stored trace
 *
 * @param candidate_trace
 * @param query_trace
 * @return std::vector<std::unordered_map<int, int>>
 */
std::vector<std::unordered_map<int, int>> get_isomorphism_mappings(
    trace_structure &candidate_trace, trace_structure &query_trace) {

    graph_type candidate_graph = morph_trace_structure_to_boost_graph_type(candidate_trace);
    graph_type query_graph = morph_trace_structure_to_boost_graph_type(query_trace);

    vertex_comp_t vertex_comp = make_property_map_equivalent_custom(
        boost::get(boost::vertex_name_t::vertex_name, query_graph),
        boost::get(boost::vertex_name_t::vertex_name, candidate_graph));

    std::vector<std::unordered_map<int, int>> isomorphism_maps;

    vf2_callback_custom<graph_type, graph_type, std::vector<std::unordered_map<int, int>>> callback(
        query_graph, candidate_graph, isomorphism_maps);

    boost::vf2_subgraph_iso(
        query_graph,
        candidate_graph,
        callback,
        boost::vertex_order_by_mult(query_graph),
        boost::vertices_equivalent(vertex_comp));
    return isomorphism_maps;
}

StatusOr<std::vector<std::string>> get_trace_ids_from_trace_hashes_object(
    const std::string &object_name, gcs::Client* client) {
    auto object_content = read_object(
        std::string(TRACE_HASHES_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX),
        object_name, client);
    if (!object_content.ok()) {
        if (object_content.status().code() == google::cloud::StatusCode::kNotFound) {
            std::vector<std::string> res;
            return res;
        }
        return object_content.status();
    }

    if (object_content.value() == "") {
        return std::vector<std::string>();
    }
    std::vector<std::string> response;
    for (auto curr_trace_id : split_by_string(object_content.value(), newline)) {
        if (curr_trace_id != "") {
            response.push_back(curr_trace_id);
        }
    }
    return response;
}

trace_structure morph_trace_object_to_trace_structure(std::string &trace) {
    trace_structure response;

    std::unordered_map<std::string, std::string> span_to_service;
    std::unordered_map<std::string, int> reverse_node_names;
    std::multimap<std::string, std::string> edges;

    for (std::string& line : split_by_string(trace, newline)) {
        if (line.substr(0, 10) == "Trace ID: ") {
            continue;
        }

        std::vector<std::string> span_info = split_by_string(line, colon);
        if (span_info.size() != 4) {
            std::cerr << "Malformed trace found: \n" << trace << std::endl;
            return response;
        }

        span_to_service.insert(std::make_pair(span_info[1], span_info[1]+":"+span_info[2]+":"+span_info[3]));

        if (span_info[0].length() > 0) {
            edges.insert(std::make_pair(span_info[0], span_info[1]));
        }
    }

    response.num_nodes = span_to_service.size();

    // Filling response.node_names
    int count = 0;
    for (const auto& elem : span_to_service) {
        response.node_names.insert(make_pair(count, elem.second));
        reverse_node_names.insert(make_pair(elem.second, count));
        count++;
    }

    // Filling response.edges
    for (const auto& elem : edges) {
        response.edges.insert(std::make_pair(
            reverse_node_names[span_to_service[elem.first]],
            reverse_node_names[span_to_service[elem.second]]));
    }

    for (auto [k, v] : response.node_names) {
        response.node_names[k] = split_by_string(v, colon)[1];
    }

    return response;
}

graph_type morph_trace_structure_to_boost_graph_type(trace_structure &input_graph) {
    graph_type output_graph;

    for (uint64_t i = 0; i < input_graph.num_nodes; i++) {
        boost::add_vertex(vertex_property(input_graph.node_names[i], i), output_graph);
    }

    for (const auto& elem : input_graph.edges) {
        boost::add_edge(elem.first, elem.second, output_graph);
    }

    return output_graph;
}

void print_trace_structure(trace_structure trace) {
    std::cout << "n: " << trace.num_nodes << std::endl;
    std::cout << "node names:" << std::endl;
    for (const auto& elem : trace.node_names) {
        std::cout << elem.first << " : " << elem.second << std::endl;
    }
    std::cout << "edges:" << std::endl;
    for (const auto& elem : trace.edges) {
        std::cout << elem.first << " : " << elem.second << std::endl;
    }
}
