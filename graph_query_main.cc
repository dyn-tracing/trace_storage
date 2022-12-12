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
    query.graph.num_nodes = 3;
    query.graph.node_names.insert(std::make_pair(0, "SparrowGalliano"));
    query.graph.node_names.insert(std::make_pair(1, "FalconHitPink"));
    query.graph.node_names.insert(std::make_pair(2, "FalconHitPink"));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(0, 2));

    // query condition
    query_condition condition1;
    condition1.node_index = 0;
    condition1.type = string_value;
    get_value_func condition_1_union;
    condition1.func = condition_1_union;
    condition1.node_property_value = "540";
    condition1.comp = Equal_to;
    condition1.property_name = "http.status_code";
    condition1.is_latency_condition = false;
    condition1.is_attribute_condition = true;

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

//     Trace ID: 0b133b8b15919239000625000eb91900:
// :a6ee0a929e4c5268:WolverineSpunPearl:12829124769
// a6ee0a929e4c5268:b00739b48110ed5f:BatPrincessPerfume:2650519354
// a6ee0a929e4c5268:eb9c0f4cf3fa3cf3:BatPrincessPerfume:2650519354
// a6ee0a929e4c5268:b90ae5ed03c8b2c3:WolverineSpunPearl:2461092713
// a6ee0a929e4c5268:3b379ccbadf6aae0:(?):2112282468
// a6ee0a929e4c5268:a908b04ceaf4f1ec:CrabYellow:493618264

QueryData canonical() {
    QueryData query;
    query.graph.num_nodes = 3;
    query.graph.node_names.insert(std::make_pair(0, "SparrowGalliano"));
    query.graph.node_names.insert(std::make_pair(1, "FalconHitPink"));
    query.graph.node_names.insert(std::make_pair(2, "FalconHitPink"));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(0, 2));

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
        QueryData data = duration_condition();
        auto time_taken = perform_query(data, true, 1670939801, 1670939801, &client);
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
