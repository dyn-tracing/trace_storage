#include "get_traces_by_structure.h"
#include "graph_query.h"
#include "indices/query_bloom_index.h"

struct QueryData {
    trace_structure graph;
    std::vector<query_condition> conditions;
    return_value ret;
};

QueryData general_graph_query() {
    QueryData query;
    // query trace structure
    query.graph.num_nodes = 3;
    query.graph.node_names.insert(std::make_pair(0, "frontend"));
    query.graph.node_names.insert(std::make_pair(1, "adservice"));
    query.graph.node_names.insert(std::make_pair(2, ASTERISK_SERVICE));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(1, 2));

    // query condition
    query_condition condition1;
    condition1.node_index = 0;
    condition1.type = int_value;
    get_value_func condition_1_union;
    condition1.func = condition_1_union;
    condition1.node_property_value = "100000000";
    condition1.comp = Greater_than;
    condition1.property_name = "duration";
    condition1.is_latency_condition = true;


    query.conditions.push_back(condition1);

    query.ret.node_index = 1;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::span_id;

    query.ret.func = ret_union;
    return query;
}

QueryData four_fan_out() {
    QueryData query;
    query.graph.num_nodes = 5;
    query.graph.node_names.insert(std::make_pair(0, "adservice"));
    query.graph.node_names.insert(std::make_pair(1, ASTERISK_SERVICE));
    query.graph.node_names.insert(std::make_pair(2, ASTERISK_SERVICE));
    query.graph.node_names.insert(std::make_pair(3, ASTERISK_SERVICE));
    query.graph.node_names.insert(std::make_pair(4, ASTERISK_SERVICE));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(0, 2));
    query.graph.edges.insert(std::make_pair(0, 3));
    query.graph.edges.insert(std::make_pair(0, 4));

    query.ret.node_index = 0;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::span_id;
    query.ret.func = ret_union;

    return query;
}

QueryData frontend_span_ids() {
    QueryData query;
    query.graph.num_nodes = 2;
    query.graph.node_names.insert(std::make_pair(0, "frontend"));
    query.graph.node_names.insert(std::make_pair(1, ASTERISK_SERVICE));
    query.graph.edges.insert(std::make_pair(0, 1));

    query.ret.node_index = 0;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::span_id;
    query.ret.func = ret_union;

    return query;
}

QueryData duration_condition() {
    QueryData query;
    query.graph.num_nodes = 2;
    query.graph.node_names.insert(std::make_pair(0, "emailservice"));
    query.graph.node_names.insert(std::make_pair(1, "recommendationservice"));
    query.graph.edges.insert(std::make_pair(0, 1));

    // query condition
    query_condition condition1;
    condition1.node_index = 0;
    condition1.type = int_value;
    get_value_func condition_1_union;
    condition1.func = condition_1_union;
    condition1.node_property_value = "400";
    condition1.comp = Greater_than;
    condition1.property_name = "duration";
    condition1.is_latency_condition = true;


    query.conditions.push_back(condition1);

    query.ret.node_index = 0;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;
    query.ret.func = ret_union;

    return query;
}

QueryData height_at_least_four() {
    QueryData query;
    query.graph.num_nodes = 4;
    query.graph.node_names.insert(std::make_pair(0, "frontend"));
    query.graph.node_names.insert(std::make_pair(1, ASTERISK_SERVICE));
    query.graph.node_names.insert(std::make_pair(2, ASTERISK_SERVICE));
    query.graph.node_names.insert(std::make_pair(3, ASTERISK_SERVICE));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(1, 2));
    query.graph.edges.insert(std::make_pair(2, 3));

    query.ret.node_index = 0;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;
    query.ret.func = ret_union;

    return query;
}

QueryData canonical() {
    QueryData query;
    query.graph.num_nodes = 3;
    query.graph.node_names.insert(std::make_pair(0, "frontend"));
    query.graph.node_names.insert(std::make_pair(1, "adservice"));
    query.graph.node_names.insert(std::make_pair(2, ASTERISK_SERVICE));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(1, 2));

    query.ret.node_index = 0;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;
    query.ret.func = ret_union;

    return query;
}

int64_t perform_query(QueryData query_data, bool verbose, time_t start_time, time_t end_time, gcs::Client* client) {
    boost::posix_time::ptime start, stop;
    
    start = boost::posix_time::microsec_clock::local_time();
    auto res = query(query_data.graph, start_time, end_time, query_data.conditions, query_data.ret, verbose, client);
    stop = boost::posix_time::microsec_clock::local_time();
    
    boost::posix_time::time_duration dur = stop - start;
    int64_t milliseconds = dur.total_milliseconds();
    std::cout << "Total results: " << res.size() << std::endl;
    return milliseconds;
}

int main(int argc, char* argv[]) {
    auto client = gcs::Client();

    // TODO(jessberg): make this choice a command line argument
    // QueryData data = four_fan_out(); // works
    // QueryData data = frontend_span_ids();
    // QueryData data = duration_condition();
    // QueryData data = height_at_least_four();
    int n = 1;
    if (argc > 1) {
        n = std::stoi(argv[1]);
    }

    std::vector<time_t> times(n, 0); 
    for (int i = 0; i < n; i++) {
        QueryData data = canonical();
        auto time_taken = perform_query(data, false, 1667248040, 1667248050, &client);
        std::cout << "Time Taken: " << time_taken << " ms\n" << std::endl;
        times[i] = time_taken;
    }
    
    // Calculate Median
    std::sort(times.begin(), times.end());
    int mid_ind = times.size()/2;
    if (0 == (times.size() % 2)) {
        auto median = (times[mid_ind-1]+times[mid_ind])/2.0;
        std::cout << "Median: " << median << std::endl;
    } else {
        std::cout << "Median: " << times[mid_ind] << std::endl;
    }
    
    return 0;
}
