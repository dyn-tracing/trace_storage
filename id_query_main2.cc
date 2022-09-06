#include "get_traces_by_structure.h"
#include "graph_query.h"
#include "query_bloom_index.h"

std::string get_trace_by_brute(std::string trace_id, int start_time, int end_time, gcs::Client* client) {
    std::string bucket_name = std::string(TRACE_STRUCT_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX);
    for (auto&& object_metadata : client->ListObjects(bucket_name)) {
        if (!object_metadata) {
            std::cerr << object_metadata.status().message() << std::endl;
            exit(1);
        }

        std::string batch_name = object_metadata->name();
        auto timestamps = extract_batch_timestamps(batch_name);
        if (timestamps.first == start_time && timestamps.second == end_time) {
            auto obj = read_object(bucket_name, object_metadata->name(), client);
            return extract_trace_from_traces_object(trace_id, obj);
        }
    }
    return "";
}

int main(int argc, char* argv[]) {
    auto trace_id = "0c4c090dfa342dfb45bc7f317bb2bbd1";
    auto start_time = 1662408274;
    auto end_time = 1662408275;

    auto client = gcs::Client();

    for (int i = 0; i < 1; i++) {
        boost::posix_time::ptime start, stop;
        start = boost::posix_time::microsec_clock::local_time();

        auto res = get_trace_by_brute(trace_id, start_time, end_time, &client);
        
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
