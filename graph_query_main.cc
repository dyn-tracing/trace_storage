#include "get_traces_by_structure.h"
#include "graph_query.h"
#include "query_bloom_index.h"

int main(int argc, char* argv[]) {
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

    // query_condition condition1;
    // condition1.node_index = 2;
    // condition1.type = int_value;
    // get_value_func condition_1_union;
    // condition_1_union.int_func = &opentelemetry::proto::trace::v1::Span::start_time_unix_nano;
    // condition1.func = condition_1_union;
    // condition1.node_property_value = "10000000";  // 1e+7 ns, 10 ms
    // condition1.comp = Lesser_than;
    // condition1.property_name = "starttime";

    // conditions.push_back(condition1);

    // querying
    auto client = gcs::Client();

    return_value ret;
    ret.node_index = 0;
    ret.type = bytes_value;
    get_value_func ret_union;
    ret_union.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;

    ret.func = ret_union;
    boost::posix_time::ptime start, stop;
	start = boost::posix_time::microsec_clock::local_time();
    
    // auto res = query(query_trace, 1653919700, 1653919710, conditions, ret, true, &client);
    auto res2 = get_traces_by_structure(query_trace, 1660239561, 1660239571, &client);
    
    stop = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration dur = stop - start;
	int64_t milliseconds = dur.total_milliseconds();
	std::cout << "Time taken: " << milliseconds << std::endl;
    std::cout << "Total results: " << res.size() << std::endl;
    return 0;
}
