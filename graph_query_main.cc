#include "get_traces_by_structure.h"
#include "graph_query.h"
#include "query_bloom_index.h"

int main(int argc, char* argv[]) {
    // query trace structure
    trace_structure query_trace;
    query_trace.num_nodes = 1;
    query_trace.node_names.insert(std::make_pair(0, "currencyservice"));
    // query_trace.node_names.insert(std::make_pair(1, "adservice"));
    // query_trace.node_names.insert(std::make_pair(2, ASTERISK_SERVICE));

    // query_trace.edges.insert(std::make_pair(0, 1));
    // query_trace.edges.insert(std::make_pair(1, 2));

    // query conditions
    std::vector<query_condition> conditions;

    // query_condition condition1;
    // condition1.node_index = 2;
    // condition1.type = int_value;
    // get_value_func condition_1_union;
    // condition_1_union.int_func = &opentelemetry::proto::trace::v1::Span::start_time_unix_nano;
    // condition1.func = condition_1_union;
    // condition1.node_property_value = "10000000";  // 1e+7 ns, 10 ms
    // condition1.comp = Lesser_than;

    // conditions.push_back(condition1);

    // querying
    auto client = gcs::Client();

    // auto total = get_traces_by_structure(query_trace, 1653317532, 1653317532, &client);
    // std::cout << "Total traces: " << total.trace_ids.size() << std::endl;
    // std::cout << "ID: " << total.trace_ids[0] << std::endl;

    return_value ret;
    auto res = query(query_trace, 1653317532, 1653317532, conditions, ret, &client);
    std::cout << "Total traces: " << res.size() << std::endl;
    std::cout << "ID: " << res[0] << std::endl;

    // std::string batch = query_bloom_index_for_value(&client, "c5367e16e960a3452529e44d035a9bec", "new_id_index");
    // std::cout << "batch " << batch << std::endl;
    return 0;
}
