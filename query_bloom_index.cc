#include "query_bloom_index.h"

std::vector<std::tuple<time_t, time_t>> get_children(std::tuple<time_t, time_t> parent, time_t granularity) {
    time_t chunk_size = (std::get<1>(parent)-std::get<0>(parent))/granularity;
    std::vector<std::tuple<time_t, time_t>> to_return;
    for (time_t i=std::get<0>(parent); i < std::get<1>(parent); i += chunk_size) {
        to_return.push_back(std::make_tuple(i, i+chunk_size));
    }
    return to_return;
}

std::vector<std::string> is_trace_id_in_leaf(
    gcs::Client* client, std::string traceID, time_t start_time, time_t end_time, std::string index_bucket) {
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
    for (int i=0; i < leaf.batch_names.size(); i++) {
        if (leaf.bloom_filters[i].contains(traceID_c_str, len)) {
            to_return.push_back(leaf.batch_names[i]);
        }
    }
    return to_return;
}

bool is_trace_id_in_nonterminal_node(
    gcs::Client* client, std::string traceID, time_t start_time, time_t end_time, std::string index_bucket
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

std::string query_bloom_index_for_value(gcs::Client* client, std::string queried_value, std::string index_bucket) {
    std::tuple<time_t, time_t> root;
    time_t granularity;
    get_root_and_granularity(client, root, granularity, index_bucket);

    std::vector<std::tuple<time_t, time_t>> unvisited_nodes;
    unvisited_nodes.push_back(root);

    // this will contain lists of batches that have the trace ID according to their bloom filters
    // this is a vector and not a single value because bloom filters may give false positives
    std::vector<std::future<std::vector<std::string>>> batches;

    // the way to parallelize this is to do all unvisited_nodes in parallel
    while (unvisited_nodes.size() > 0) {
        std::vector<std::tuple<time_t, time_t>> new_unvisited;
        std::vector<std::future<bool>> got_positive;
        std::vector<std::tuple<time_t, time_t>> got_positive_limits;
        for (int i=0; i < unvisited_nodes.size(); i++) {
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
        for (int i=0; i < got_positive.size(); i++) {
            if (got_positive[i].get()) {
                auto children = get_children(got_positive_limits[i], granularity);
                for (int j=0; j < children.size(); j++) {
                    new_unvisited.push_back(children[j]);
                }
            }
        }
        unvisited_nodes.clear();
        for (int i=0; i < new_unvisited.size(); i++) {
            unvisited_nodes.push_back(new_unvisited[i]);
        }
    }
    // now figure out which of the batches actually have your trace ID
    // because false positives are a thing, this could potentially be more than one batch that shows up true
    std::vector<std::string> verified_batches;
    for (int i=0; i < batches.size(); i++) {
        std::vector<std::string> verified = batches[i].get();
        for (int j=0; j < verified.size(); j++) {
            verified_batches.push_back(verified[j]);
        }
    }

    // this is the common case:  no false positives
    if (verified_batches.size() == 1) {
        return verified_batches[0];
    }

    // else we need to actually look up the trace structure objects to differentiate
    std::string trace_struct_bucket(TRACE_STRUCT_BUCKET_PREFIX);
    std::string suffix(SERVICES_BUCKETS_SUFFIX);
    trace_struct_bucket += suffix;
    for (int i=0; i < verified_batches.size(); i++) {
        auto reader = client->ReadObject(trace_struct_bucket, verified_batches[i]);
        if (!reader) {
            std::cerr << "Error reading object: " << reader.status() << "\n";
            throw std::runtime_error("Error reading trace object");
        } else {
            std::string contents{std::istreambuf_iterator<char>{reader}, {}};
            if (contents.find(queried_value) != -1) {
                return verified_batches[i];
            }
        }
    }
    return "";
}
