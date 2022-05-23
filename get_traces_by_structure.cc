#include "get_traces_by_structure.h"

std::vector<traces_by_structure> get_traces_by_structure(
    trace_structure query_trace, int start_time, int end_time, gcs::Client* client) {
    std::vector<std::future<traces_by_structure>> response_futures;

    std::string prefix(TRACE_HASHES_BUCKET_PREFIX);
    std::string suffix(SERVICES_BUCKETS_SUFFIX);

    for (auto&& prefix : client->ListObjectsAndPrefixes(prefix+suffix, gcs::Delimiter("/"))) {
        if (!prefix) {
            std::cerr << "Error in getting prefixes" << std::endl;
            exit(1);
        }

        auto result = *std::move(prefix);
        if (false == absl::holds_alternative<std::string>(result)) {
            std::cerr << "Error in getting prefixes" << std::endl;
            exit(1);
        }
        std::string prefix_ = absl::get<std::string>(result);

        response_futures.push_back(std::async(
            std::launch::async, process_trace_hashes_prefix_and_retrieve_relevant_trace_ids,
            prefix_, query_trace, start_time, end_time, client));
    }

    std::vector<traces_by_structure> response;
    for_each(response_futures.begin(), response_futures.end(),
        [&response](std::future<traces_by_structure>& fut){
            traces_by_structure to_return = fut.get();
            response.push_back(to_return);
    });
    return response;
}

traces_by_structure process_trace_hashes_prefix_and_retrieve_relevant_trace_ids(
    std::string prefix, trace_structure query_trace, int start_time, int end_time,
    gcs::Client* client
) {
    traces_by_structure to_return;

    std::string suffix(SERVICES_BUCKETS_SUFFIX);
    std::string trace_hashes_bucket(TRACE_HASHES_BUCKET_PREFIX);

    for (auto&& object_metadata : client->ListObjects(trace_hashes_bucket+suffix, gcs::Prefix(prefix))) {
        if (!object_metadata) {
            std::cerr << object_metadata.status().message() << std::endl;
            exit(1);
        }

        std::string object_name = object_metadata->name();
        std::string batch_name = extract_batch_name(object_name);

        std::pair<int, int> batch_time = extract_batch_timestamps(batch_name);
        if (false == is_object_within_timespan(batch_time, start_time, end_time)) {
            continue;
        }

        std::vector<std::string> response_trace_ids = get_trace_ids_from_trace_hashes_object(object_name, client);
        if (response_trace_ids.size() < 1) {
            continue;
        }

        std::string object_content = read_object(trace_hashes_bucket+suffix, batch_name, client);
        if (object_content == "") {
            continue;
        }

        std::string trace = extract_any_trace(response_trace_ids, object_content);
        if (trace == "") {
            continue;
        }

        trace_structure candidate_trace = morph_trace_object_to_trace_structure(trace);
        if (candidate_trace.num_nodes < 1) {
            continue;
        }

        if (to_return.iso_maps.size() < 1) {
            to_return.iso_maps = get_isomorphism_mappings(candidate_trace, query_trace);
            if (to_return.iso_maps.size() < 1) {
                return to_return;
            }
        }

        response_trace_ids = filter_trace_ids_based_on_query_timestamp(
            response_trace_ids, batch_name, object_content, start_time, end_time, client);
        if (response_trace_ids.size() < 1) {
            continue;
        }

        to_return.obj_to_trace_ids.push_back(std::make_tuple(batch_name, response_trace_ids));
    }

    return to_return;
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
    trace_structure candidate_trace, trace_structure query_trace) {
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

std::vector<std::string> get_trace_ids_from_trace_hashes_object(std::string object_name, gcs::Client* client) {
    std::string trace_hashes_bucket(TRACE_HASHES_BUCKET_PREFIX);
    std::string suffix(SERVICES_BUCKETS_SUFFIX);
    std::string object_content = read_object(trace_hashes_bucket+suffix, object_name, client);
    if (object_content == "") {
        return std::vector<std::string>();
    }
    std::vector<std::string> trace_ids = split_by_string(object_content, newline);

    return trace_ids;
}

trace_structure morph_trace_object_to_trace_structure(std::string trace) {
    trace_structure response;

    std::vector<std::string> trace_lines = split_by_string(trace, newline);
    std::unordered_map<std::string, std::string> span_to_service;
    std::unordered_map<std::string, int> reverse_node_names;
    std::multimap<std::string, std::string> edges;

    for (auto line : trace_lines) {
        if (line.substr(0, 10) == "Trace ID: ") {
            continue;
        }

        std::vector<std::string> span_info = split_by_string(line, colon);
        if (span_info.size() != 4) {
            std::cerr << "Malformed trace found: \n" << trace << std::endl;
            return response;
        }

        span_to_service.insert(std::make_pair(span_info[1], span_info[2]+":"+span_info[3]));

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

    return response;
}

graph_type morph_trace_structure_to_boost_graph_type(trace_structure input_graph) {
    graph_type output_graph;

    for (int i = 0; i < input_graph.num_nodes; i++) {
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
