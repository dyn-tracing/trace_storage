#include "get_traces_by_structure.h"
#include "graph_query.h"
#include "indices/query_bloom_index.h"

int64_t general_graph_query(bool verbose, gcs::Client* client) {
    // query trace structure
    trace_structure query_trace;
    query_trace.num_nodes = 3;
    query_trace.node_names.insert(std::make_pair(0, "frontend"));
    query_trace.node_names.insert(std::make_pair(1, "adservice"));
    query_trace.node_names.insert(std::make_pair(2, ASTERISK_SERVICE));

    query_trace.edges.insert(std::make_pair(0, 1));
    query_trace.edges.insert(std::make_pair(1, 2));

    // query conditions
    std::vector<query_condition> conditions;

    query_condition condition1;
    condition1.node_index = 0;
    condition1.type = int_value;
    get_value_func condition_1_union;
    condition1.func = condition_1_union;
    condition1.node_property_value = "100000000";
    condition1.comp = Greater_than;
    condition1.property_name = "duration";
    condition1.is_latency_condition = true;


    //conditions.push_back(condition1);

    // querying
    auto client = gcs::Client();


    return_value ret;
    ret.node_index = 1;
    ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::span_id;

    ret.func = ret_union;
    boost::posix_time::ptime start, stop;
	  start = boost::posix_time::microsec_clock::local_time();
    auto res = query(query_trace, 1666822600, 1666822800, conditions, ret, true, &client);
    stop = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration dur = stop - start;
	int64_t milliseconds = dur.total_milliseconds();
    std::cout << "Total results: " << res.size() << std::endl;
    return milliseconds;
}

int64_t simple_graph_query(bool verbose, gcs::Client* client) {
    // query trace structure
    trace_structure query_trace;
    query_trace.num_nodes = 3;
    query_trace.node_names.insert(std::make_pair(0, "frontend"));
    query_trace.node_names.insert(std::make_pair(1, "adservice"));
    query_trace.node_names.insert(std::make_pair(2, ASTERISK_SERVICE));

    query_trace.edges.insert(std::make_pair(0, 1));
    query_trace.edges.insert(std::make_pair(1, 2));

    return_value ret;
    ret.node_index = 0;
    ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;

    ret.func = ret_union;
    boost::posix_time::ptime start, stop;
	  start = boost::posix_time::microsec_clock::local_time();
    auto res = query(query_trace, 1666822600, 1666822800, conditions, ret, true, &client);
    stop = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration dur = stop - start;
	int64_t milliseconds = dur.total_milliseconds();
    std::cout << "Total results: " << res.size() << std::endl;
    return milliseconds;
}

int main(int argc, char* argv[]) {
    auto client = gcs::Client();
    auto time_taken = general_graph_query(false, &client);
    std::cout << "Time Taken: " << time_taken << " ms" << std::endl;
    return 0;
}
