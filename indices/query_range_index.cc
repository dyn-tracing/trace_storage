#include <stdlib.h>

#include "make_range_index/nodes.h"

Status get_last_updated_and_time_range_per_node_and_nodes_per_summary(
    gcs::Client* client, time_t last_updated, time_t time_range_per_node,
    int64_t nodes_per_summary, std::string index_bucket) {

}

std::vector<std::string> calculate_summaries_to_retrieve(
    time_t start_time, time_t end_time, time_t last_updated,
    time_t time_range_per_node, time_t nodes_per_summary) {


}

objname_to_matching_trace_ids get_traces_matching_query(
    gcs::Client* client, std::string summary_name, time_t start_time, time_t end_time,
    time_t last_updated, std::string query_value) {

}

StatusOr<objname_to_matching_trace_ids> query_range_index_for_value(
    gcs::Client* client, std::string queried_value, std::string index_bucket,
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
            client, summaries[i], start_time, end_time, last_updated, queried_value));
    }

    objname_to_matching_trace_ids to_return;
    for (int64_t i=0; i < traces_matching_query.size(); i++) {
        objname_to_matching_trace_ids small_batch = traces_matching_query[i].get();
        // TODO: merge

    }
    return to_return;
}
