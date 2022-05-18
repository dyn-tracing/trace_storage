#include "graph_query.h"

int main(int argc, char* argv[]) {
    dummy_tests();

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
    condition1.node_index = 2;
    condition1.type = int_value;
    get_value_func condition_1_union;
    condition_1_union.int_func = &opentelemetry::proto::trace::v1::Span::start_time_unix_nano;
    condition1.func = condition_1_union;
    condition1.node_property_value = "10000000";  // 1e+7 ns, 10 ms
    condition1.comp = Lesser_than;

    conditions.push_back(condition1);

    // querying
    auto client = gcs::Client();
    std::vector<std::string> total = get_traces_by_structure(query_trace, 1651696797, 1651696798, conditions, &client);
    std::cout << "Total results: " << total.size() << std::endl;
    return 0;
}
