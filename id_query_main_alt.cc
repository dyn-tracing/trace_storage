#include "get_traces_by_structure.h"
#include "graph_query.h"
#include "query_bloom_index.h"

std::string fetch_obj_name_from_index(std::string trace_id, int start_time, int end_time, gcs::Client* client) {
    query_condition qc;
    qc.property_name = "trace-id";

    auto indexed = is_indexed(&qc, client);
    index_type i_type = std::get<0>(indexed);
    if (i_type != index_type::bloom) {
        std::cerr << "Not good!" << std::endl;
        exit(1);
    }

    std::string bucket_name = get_index_bucket_name(qc.property_name);
    auto res = query_bloom_index_for_value(client, trace_id, bucket_name, start_time, end_time);
    
    if (res.size() != 1) {
        std::cerr << "Shouldn't happen!" << std::endl;
        exit(1);
    }
    
    for (auto [obj_name, trace_ids] : res) {
        return obj_name;
    }
    return "";
}

int main(int argc, char* argv[]) {
    auto trace_id = "a27abcf29be19908eba8298db950cf1e";
    auto start_time = 1662226680;
    auto end_time = 1662226681;

    auto client = gcs::Client();

    for (int i = 0; i < 20; i++) {
        boost::posix_time::ptime start, stop;
        start = boost::posix_time::microsec_clock::local_time();

        auto obj_name = fetch_obj_name_from_index(trace_id, start_time, end_time, &client);
        auto obj = read_object(TRACE_STRUCT_BUCKET, obj_name, &client);
        auto res = extract_trace_from_traces_object(trace_id, obj);
        
        stop = boost::posix_time::microsec_clock::local_time();
        boost::posix_time::time_duration dur = stop - start;
        int64_t milliseconds = dur.total_milliseconds();

        std::cout << milliseconds << std::endl;
        if (res.empty()) {
            std::cout << "Couldn't get it!" << std::endl;
        }
    }
    return 0;
}
