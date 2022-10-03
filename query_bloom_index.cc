#include "query_bloom_index.h"

std::vector<std::tuple<time_t, time_t>> get_children(
    const std::tuple<time_t, time_t> parent, const time_t granularity) {
    time_t chunk_size = (std::get<1>(parent)-std::get<0>(parent))/granularity;
    std::vector<std::tuple<time_t, time_t>> to_return;
    for (time_t i=std::get<0>(parent); i < std::get<1>(parent); i += chunk_size) {
        to_return.push_back(std::make_tuple(i, i+chunk_size));
    }
    return to_return;
}

std::vector<std::string> is_trace_id_in_leaf(
    gcs::Client* client, const std::string traceID, const time_t start_time,
    const time_t end_time, const std::string index_bucket) {
    std::vector<std::string> to_return;
    const char* traceID_c_str = traceID.c_str();
    size_t len = traceID.length();
    std::string leaf_name = std::to_string(start_time) + "-" + std::to_string(end_time);
    auto reader = client->ReadObject(index_bucket, leaf_name);
    if (reader.status().code() == ::google::cloud::StatusCode::kNotFound) {
        return to_return;  // if it doesn't exist, then you can't get the trace there
    } else if (!reader) {
        std::cerr << "Error reading object: " << reader.status() << "\n";
        throw std::runtime_error("Error reading leaf object");
    }
    Leaf leaf = deserialize_leaf(reader);
    for (uint64_t i=0; i < leaf.batch_names.size(); i++) {
        if (leaf.bloom_filters[i].contains(traceID_c_str, len)) {
            to_return.push_back(leaf.batch_names[i]);
        }
    }
    return to_return;
}

bool is_trace_id_in_nonterminal_node(
    gcs::Client* client, const std::string traceID, const time_t start_time,
    const time_t end_time, const std::string index_bucket
) {
    std::string bloom_filter_name = std::to_string(start_time) + "-" + std::to_string(end_time);
    auto reader = client->ReadObject(index_bucket, bloom_filter_name);
    if (reader.status().code() == ::google::cloud::StatusCode::kNotFound) {
        return false;  // if it doesn't exist, then you can't get the trace there
    }
    if (!reader) {
        std::cerr << "Error reading object: " << reader.status() << bloom_filter_name << "\n";
        throw std::runtime_error("Error reading node object");
    }
    bloom_filter bf;
    bf.Deserialize(reader);
    const char* traceID_c_str = traceID.c_str();
    size_t len = traceID.length();
    return bf.contains(traceID_c_str, len);
}

objname_to_matching_trace_ids get_return_value_from_objnames(gcs::Client* client,
    std::vector<std::string> object_names,
    std::string index_bucket, std::string queried_value) {

    objname_to_matching_trace_ids to_return;
    if (index_bucket.compare(TRACE_ID_BUCKET) == 0) {
        for (uint64_t i=0; i < object_names.size(); i++) {
            auto reader = client->ReadObject(TRACE_STRUCT_BUCKET, object_names[i]);
            if (!reader) {
                std::cerr << "Error reading object: " << reader.status() << object_names[i] << "\n";
                throw std::runtime_error("Error reading node object");
            }
            std::string contents{std::istreambuf_iterator<char>{reader}, {}};
            if (contents.find(queried_value) != std::string::npos) {
                to_return[object_names[i]].push_back(queried_value);
            }
        }
    } else if (index_bucket.compare(SPAN_ID_BUCKET) == 0) {
        for (uint64_t i=0; i < object_names.size(); i++) {
            auto reader = client->ReadObject(TRACE_STRUCT_BUCKET, object_names[i]);
            if (!reader) {
                std::cerr << "Error reading object: " << reader.status() << object_names[i] << "\n";
                throw std::runtime_error("Error reading node object");
            }
            std::string contents{std::istreambuf_iterator<char>{reader}, {}};
            std::size_t index = contents.find(queried_value);
            if (index != std::string::npos) {
                int trace_id_index = contents.rfind("Trace ID", index);
                std::string trace_id = contents.substr(trace_id_index + 10, TRACE_ID_LENGTH);
                to_return[object_names[i]].push_back(trace_id);
            }
        }
    } else {
        for (uint64_t i=0; i < object_names.size(); i++) {
            auto reader = client->ReadObject(TRACE_STRUCT_BUCKET, object_names[i]);
            if (!reader) {
                std::cerr << "Error reading object: " << reader.status() << object_names[i] << "\n";
                throw std::runtime_error("Error reading node object");
            }
            std::string contents{std::istreambuf_iterator<char>{reader}, {}};
            std::vector<std::string> lines = split_by_string(contents, newline);
            for (uint64_t j=0; j < lines.size(); j++) {
                std::size_t trace_id_index = lines[j].find("Trace ID");
                if (trace_id_index != std::string::npos) {
                    std::string trace_id = contents.substr(trace_id_index+10, TRACE_ID_LENGTH);
                    if (std::find(to_return[object_names[i]].begin(),
                                  to_return[object_names[i]].end(),
                                  trace_id)
                        == to_return[object_names[i]].end()) {
                        to_return[object_names[i]].push_back(trace_id);
                    }
                }
            }
        }
    }
    return to_return;
}


std::tuple<time_t, time_t> get_nearest_node(std::tuple<time_t, time_t> root, time_t granularity,
    time_t start_time, time_t end_time) {
    // we want to find the node with the nearest range; this is equivalent to
    // doing in order traversal of the tree defined by the root and granularity.
    std::tuple<time_t, time_t> curr = root;
    bool child_also_has_range = false;
    do {
        child_also_has_range = false;
        for (auto & child : get_children(curr, granularity)) {
            if (!child_also_has_range && std::get<0>(child) <= start_time && std::get<1>(child) >= end_time) {
                curr = child;
                child_also_has_range = true;
            }
        }
    } while (std::get<0>(curr) <= start_time && std::get<1>(curr) >= end_time &&
           std::get<1>(curr) - std::get<0>(curr) > granularity && child_also_has_range);
    return curr;
}


// Right now, I return the object name -> all trace IDs in that object, because
// the Bloom filter index does not distinguish on a trace-by-trace level
// trace ID queries and span ID are the exception;  those may be inferred with a single GET.
// so it's actually more efficient for the index to return what may be a superset
objname_to_matching_trace_ids query_bloom_index_for_value(
    gcs::Client* client, std::string queried_value, std::string index_bucket, const time_t start_time,
    const time_t end_time) {
    std::tuple<time_t, time_t> root;
    time_t granularity;
    get_root_and_granularity(client, root, granularity, index_bucket);

    std::vector<std::tuple<time_t, time_t>> unvisited_nodes;
    unvisited_nodes.push_back(get_nearest_node(root, granularity, start_time, end_time));

    // this will contain lists of batches that have the trace ID according to their bloom filters
    // this is a vector and not a single value because bloom filters may give false positives
    std::vector<std::future<std::vector<std::string>>> batches;

    // the way to parallelize this is to do all unvisited_nodes in parallel
    while (unvisited_nodes.size() > 0) {
        std::vector<std::tuple<time_t, time_t>> new_unvisited;
        std::vector<std::future<bool>> got_positive;
        std::vector<std::tuple<time_t, time_t>> got_positive_limits;
        for (uint64_t i=0; i < unvisited_nodes.size(); i++) {
            auto visit = unvisited_nodes[i];
            // process
            if (std::get<1>(visit)-std::get<0>(visit) == granularity) {
                // hit a leaf
                batches.push_back(std::async(std::launch::async, is_trace_id_in_leaf,
                    client, queried_value, std::get<0>(visit), std::get<1>(visit), index_bucket));
            } else {
                // async call if it is in nonterminal node
                got_positive_limits.push_back(visit);
                got_positive.push_back(std::async(std::launch::async, is_trace_id_in_nonterminal_node,
                    client, queried_value, std::get<0>(visit), std::get<1>(visit), index_bucket));
            }
        }
        // now we need to see how many of the non-terminal nodes showed up positive
        for (uint64_t i=0; i < got_positive.size(); i++) {
            if (got_positive[i].get()) {
                auto children = get_children(got_positive_limits[i], granularity);
                for (uint64_t j=0; j < children.size(); j++) {
                    new_unvisited.push_back(children[j]);
                }
            }
        }
        unvisited_nodes.clear();
        for (uint64_t i=0; i < new_unvisited.size(); i++) {
            unvisited_nodes.push_back(new_unvisited[i]);
        }
    }
    // now figure out which of the batches actually have your trace ID
    // because false positives are a thing, this could potentially be more than one batch that shows up true
    std::vector<std::string> verified_batches;
    for (uint64_t i=0; i < batches.size(); i++) {
        std::vector<std::string> verified = batches[i].get();
        for (uint64_t j=0; j < verified.size(); j++) {
            verified_batches.push_back(verified[j]);
        }
    }

    return get_return_value_from_objnames(client, verified_batches, index_bucket, queried_value);
}
