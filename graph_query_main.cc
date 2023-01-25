#include "get_traces_by_structure.h"
#include "graph_query.h"
#include "indices/query_bloom_index.h"

struct QueryData {
    trace_structure graph;
    std::vector<query_condition> conditions;
    return_value ret;
};



std::string fetch_obj_name_from_index(std::string trace_id, int start_time, int end_time, gcs::Client* client) {
    StatusOr<objname_to_matching_trace_ids> res_tup = query_bloom_index_for_value(client,
        trace_id, "index-trace-id-quest-new-one-csv", start_time, end_time);
    if (!res_tup.ok()) {
        std::cout << "am sad" << std::endl;
        return "";
    }
    for (auto& [objname, trace_ids] : res_tup.value()) {
        return objname;
    }
    return "";
}

QueryData plain_trace_id_query() {
    QueryData query;
    query.graph.num_nodes = 1;
    query.graph.node_names.insert(std::make_pair(0, "frontend"));

    query_condition condition1;
    condition1.node_index = 0;
    condition1.type = bytes_value;
    get_value_func condition_1_union;
    condition_1_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;
    condition1.func = condition_1_union;
    condition1.node_property_value = "b83b2deb88a6e20424d89985c2bf97b7";
    condition1.property_name = "trace-id";
    condition1.comp = Equal_to;
    query.conditions.push_back(condition1);

    query.ret.node_index = 0;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;

    query.ret.func = ret_union;
    return query;
}

QueryData trace_id_query() {
    QueryData query;
    // query trace structure
    query.graph.num_nodes = 3;
    query.graph.node_names.insert(std::make_pair(0, "KoupreyMing"));
    query.graph.node_names.insert(std::make_pair(1, "FalconHitPink"));
    query.graph.node_names.insert(std::make_pair(2, "FalconHitPink"));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(0, 2));


    query_condition condition1;
    condition1.node_index = 0;
    condition1.type = int_value;
    get_value_func condition_1_union;
    condition_1_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;
    condition1.func = condition_1_union;
    condition1.node_property_value = "0b14315f15919239692554000e52bd00";
    query.conditions.push_back(condition1);

    query.ret.node_index = 1;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;

    query.ret.func = ret_union;
    return query;
}

// Done
QueryData four_fan_out() {
    QueryData query;
    query.graph.num_nodes = 5;
    query.graph.node_names.insert(std::make_pair(0, "OryxGreenSmoke"));
    query.graph.node_names.insert(std::make_pair(1, "WolfTowerGray"));
    query.graph.node_names.insert(std::make_pair(2, "GiraffeOldBrick"));
    query.graph.node_names.insert(std::make_pair(3, "OryxGreenSmoke"));
    query.graph.node_names.insert(std::make_pair(4, "MissingService"));
    // query.graph.node_names.insert(std::make_pair(5, "MartenPersianOrange"));
    // query.graph.node_names.insert(std::make_pair(6, "MissingService"));
    // query.graph.node_names.insert(std::make_pair(7, "KingfisherUltramarineBlue"));
    // query.graph.node_names.insert(std::make_pair(8, "ElkCrimsonGlory"));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(0, 2));
    query.graph.edges.insert(std::make_pair(0, 3));
    query.graph.edges.insert(std::make_pair(0, 4));
    // query.graph.edges.insert(std::make_pair(0, 5));
    // query.graph.edges.insert(std::make_pair(0, 6));
    // query.graph.edges.insert(std::make_pair(0, 7));
    // query.graph.edges.insert(std::make_pair(0, 8));

    query.ret.node_index = 0;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;
    query.ret.func = ret_union;

    return query;
}

// Done
QueryData service_calls_one_other() {
    QueryData query;
    query.graph.num_nodes = 2;
    query.graph.node_names.insert(std::make_pair(0, "OryxGreenSmoke"));
    query.graph.node_names.insert(std::make_pair(1, "WolfTowerGray"));

    query.graph.edges.insert(std::make_pair(0, 1));

    query.ret.node_index = 0;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;
    query.ret.func = ret_union;

    return query;
}

QueryData service_calls_one_other_online_boutique() {
    QueryData query;
    query.graph.num_nodes = 2;
    query.graph.node_names.insert(std::make_pair(0, "frontend"));
    query.graph.node_names.insert(std::make_pair(1, "adservice"));

    query.graph.edges.insert(std::make_pair(0, 1));

    query.ret.node_index = 0;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;
    query.ret.func = ret_union;

    return query;
}

// done
QueryData duration_condition() {
    QueryData query;
    query.graph.num_nodes = 3;
    query.graph.node_names.insert(std::make_pair(0, "OryxGreenSmoke"));
    query.graph.node_names.insert(std::make_pair(1, "WolfTowerGray"));
    query.graph.node_names.insert(std::make_pair(2, "GiraffeOldBrick"));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(0, 2));

    // query condition
    query_condition condition1;
    condition1.node_index = 0;
    condition1.type = string_value;
    get_value_func condition_1_union;
    condition1.func = condition_1_union;
    condition1.node_property_value = "310000000";  // 300 ms
    condition1.comp = Less_than;
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

// Done
QueryData height_at_least_four() {
    QueryData query;
    query.graph.num_nodes = 4;
    query.graph.node_names.insert(std::make_pair(0, "KoupreyMing"));
    query.graph.node_names.insert(std::make_pair(1, "StingrayMabel"));
    query.graph.node_names.insert(std::make_pair(2, "QueleaSinopia"));
    query.graph.node_names.insert(std::make_pair(3, "MissingService"));

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

QueryData find_all_frontends() {
    QueryData query;
    return query;
}


QueryData general_graph_query() {
    QueryData query;
    // query trace structure
    query.graph.num_nodes = 3;
    query.graph.node_names.insert(std::make_pair(0, "ElkCrimsonGlory"));
    query.graph.node_names.insert(std::make_pair(1, "BatSkyMagenta"));
    query.graph.node_names.insert(std::make_pair(2, "MartenPersianOrange"));

    query.graph.edges.insert(std::make_pair(0, 1));
    query.graph.edges.insert(std::make_pair(0, 2));

    query.ret.node_index = 1;
    query.ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::span_id;

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

int64_t perform_trace_query(std::string trace_id, time_t start_time, time_t end_time, gcs::Client* client) {
    boost::posix_time::ptime start, stop;
    start = boost::posix_time::microsec_clock::local_time();

    std::string res = fetch_obj_name_from_index(trace_id, start_time, end_time, client);
    stop = boost::posix_time::microsec_clock::local_time();

    boost::posix_time::time_duration dur = stop - start;
    int64_t milliseconds = dur.total_milliseconds();
    std::cout << "Results: " << res << std::endl;
    return milliseconds;
}

int main(int argc, char* argv[]) {
    auto client = gcs::Client();

    QueryData data = duration_condition();
    int n = 1;
    if (argc > 1) {
        n = std::stoi(argv[1]);
    }

    if (argc > 2) {
        auto q = std::string(argv[2]);
        if (q == "fanout") {
            data = four_fan_out();
            std::cout << "Running four_fan_out()" << std::endl;
        } else if (q == "one_call") {
            data = service_calls_one_other();
            std::cout << "Running service_calls_one_other()" << std::endl;
        } else if (q == "duration") {
            data = duration_condition();
            std::cout << "Running duration_condition()" << std::endl;
        } else if (q == "height") {
            data = height_at_least_four();
            std::cout << "Running height_at_least_four()" << std::endl;
        } else if (q == "ob") {
            data = service_calls_one_other_online_boutique();
            std::cout << "Running service_calls_one_other_online_boutique()" << std::endl;
        }
    }

    std::vector<time_t> times(n, 0);
    for (int i = 0; i < n; i++) {
        int64_t time_taken;
        if (argc > 2 && std::string(argv[2]) == "trace_id") {
            time_taken = perform_trace_query("b83b2deb88a6e20424d89985c2bf97b7", 1674666130, 1674666131, &client);
        } else {
            time_taken = perform_query(data, true, 1674666130, 1674666131, &client);
        }
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
