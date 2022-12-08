#include "query_range_index.h"

int main() {
    auto client = gcs::Client();
    query_condition condition;
    condition.node_index = 0;
    condition.type = int_value;
    get_value_func condition_union;
    condition.func = condition_union;
    condition.node_property_value = "100000000";
    condition.comp = Greater_than;
    condition.property_name = "latency";
    condition.is_latency_condition = true;
    StatusOr<objname_to_matching_trace_ids> res = query_range_index_for_value(
        &client, condition,
        std::string("latency-range-index"), 1669921900, 1669922000);
    if (!res.ok()) {
        std::cout << res.status() << std::endl;
    } else {
        std::cout << res->size() << std::endl;
    }
    return 0;
}
