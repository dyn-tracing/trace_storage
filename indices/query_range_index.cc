#include <stdlib.h>

#include "query_range_index.h"
#include "make_range_index/nodes.h"
#include "common.h"
#include "query_conditions.h"


Status get_last_updated_and_time_range_per_node_and_nodes_per_summary(
    gcs::Client* client, time_t last_updated, time_t time_range_per_node,
    int64_t nodes_per_summary, std::string index_bucket) {
    StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->GetBucketMetadata(index_bucket);
    if (!bucket_metadata) {
        throw std::runtime_error(bucket_metadata.status().message());
        return bucket_metadata.status();
    }
    for (auto const& kv : bucket_metadata->labels()) {
        if (kv.first == "last_updated") {
            last_updated = time_t_from_string(kv.second);
        }
        if (kv.first == "time_range_per_node") {
            time_range_per_node = time_t_from_string(kv.second);
        }
        if (kv.first == "nodes_per_summary") {
            nodes_per_summary = strtoll(kv.second.c_str(), NULL, 10);
        }
    }
    return Status();
}

std::vector<std::string> calculate_summaries_to_retrieve(
    time_t start_time, time_t end_time, time_t last_updated,
    time_t time_range_per_node, int64_t nodes_per_summary) {

    time_t time_per_summary = time_range_per_node * nodes_per_summary;

    time_t start_summary = start_time - (start_time % time_per_summary);
    time_t end_summary = end_time - (end_time % time_per_summary);

    std::vector<std::string> to_return;
    for (time_t block = start_summary; block <= end_summary; block += time_per_summary) {
        to_return.push_back(std::to_string(block) +
                            "-" +
                            std::to_string(block+time_per_summary));
    }
    return to_return;
}

objname_to_matching_trace_ids get_traces_matching_query_in_node(
    gcs::Client* client, const std::string node_name,
    const std::string index_bucket,
    const query_condition condition,
    const time_t start_time, const time_t end_time) {
    objname_to_matching_trace_ids to_return;

    auto reader = client->ReadObject(index_bucket, node_name);
    if (!reader) {
        std::cout << "Unable to retrieve summary object." << std::endl;
        return to_return;
    }
    Node node;
    node.Deserialize(reader);

    for (int64_t i=0; i < node.data.size(); i++) {
        // 1. Is it within the time period I am interested in?
        // 2. Is it the data I am interested in?
        std::pair<int, int> batch_timestamps = extract_batch_timestamps(node.data[i].batch_name);
        if (is_object_within_timespan(batch_timestamps, start_time, end_time) &&
            does_value_satisfy_condition(node.data[i].data, condition)
        ) {
            if (to_return.find(node.data[i].batch_name) == to_return.end()) {
                std::vector<std::string> trace_ids;
                trace_ids.push_back(node.data[i].trace_id);
                to_return[node.data[i].batch_name] = trace_ids;
            } else {
                to_return[node.data[i].batch_name].push_back(node.data[i].trace_id);
            }
        }
    }
    return to_return;
}

objname_to_matching_trace_ids get_traces_matching_query(
    gcs::Client* client, std::string summary_name, time_t start_time,
    time_t end_time, time_t last_updated, const query_condition condition,
    const std::string &index_bucket, time_t time_period_per_node) {
    objname_to_matching_trace_ids to_return;
    // First, retrieve the summary object.
    auto reader = client->ReadObject(index_bucket, summary_name);
    if (!reader) {
        std::cout << "Unable to retrieve summary object." << std::endl;
        return to_return;
    }
    NodeSummary ns;
    ns.Deserialize(reader);

    // Now that we have the node summary, look through all relevant nodes
    // according to query_value.
    std::vector<std::future<objname_to_matching_trace_ids>> future_traces;
    for (int64_t i = 0; i < ns.node_objects.size(); i++) {
        // Do we want to look at this node's data?
        // TODO: is this right?
        time_t node_start_time = std::get<0>(ns.node_objects[i]);
        std::string data = std::get<1>(ns.node_objects[i]);
        if (does_value_satisfy_condition(data, condition)) {
            future_traces.push_back(std::async(std::launch::async,
                get_traces_matching_query_in_node,
                client,
                std::to_string(node_start_time) + "-" +
                std::to_string(node_start_time+time_period_per_node),
                index_bucket, condition, start_time, end_time));
        }
    }
    for (int64_t i=0; i < future_traces.size(); i++) {
        objname_to_matching_trace_ids traces = future_traces[i].get();
        merge_objname_to_trace_ids(to_return, traces);
    }
    return to_return;
}

StatusOr<objname_to_matching_trace_ids> query_range_index_for_value(
    gcs::Client* client, query_condition condition, std::string index_bucket,
    time_t start_time, time_t end_time) {

    time_t last_updated = 0, time_range_per_node = 0;
    int64_t nodes_per_summary = 0;

    Status ret = get_last_updated_and_time_range_per_node_and_nodes_per_summary(
        client, last_updated, time_range_per_node, nodes_per_summary, index_bucket);
    if (!ret.ok()) {
        return ret;
    }

    // Using the index and start_time and end_time, figure out the summary
    // objects to retrieve
    std::vector<std::string> summaries = calculate_summaries_to_retrieve(
        start_time, end_time, last_updated, time_range_per_node, nodes_per_summary);

    // Now retrieve and calculate which actual objects to retrieve
    std::vector<std::future<objname_to_matching_trace_ids>> traces_matching_query;
    for (int64_t i=0; i < summaries.size(); i++) {
        traces_matching_query.push_back(std::async(std::launch::async, get_traces_matching_query,
            client, summaries[i], start_time, end_time, last_updated, condition, index_bucket,
            time_range_per_node));
    }

    objname_to_matching_trace_ids to_return;
    for (int64_t i=0; i < traces_matching_query.size(); i++) {
        objname_to_matching_trace_ids small_batch = traces_matching_query[i].get();
        merge_objname_to_trace_ids(to_return, small_batch);
    }
    return to_return;
}
