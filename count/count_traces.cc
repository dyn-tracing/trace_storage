#include "count/count_traces.h"

#include <unordered_set>
#include <vector>

#include "common.h"

struct Counts {
    std::unordered_set<std::string> traces;
    std::unordered_set<std::string> spans;
};

Counts get_counts_for_object(std::string object, gcs::Client* client) {
    Counts to_return;
    auto reader = client->ReadObject(
        std::string(TRACE_STRUCT_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX), object);
    if (!reader) {
        std::cerr << "Error getting object " << reader.status().code() << std::endl;
    }
    std::string object_content{std::istreambuf_iterator<char>{reader}, {}};
    for (std::string& line : split_by_string(object_content, newline)) {
        if (line.compare("") == 0) {
            continue;
        }
        if (line.substr(0, 10) == "Trace ID: ") {
            to_return.traces.insert(line.substr(10, line.size()));
        }
        std::vector<std::string> span_line = split_by_string(line, colon);
        to_return.spans.insert(span_line[1]);
    }
    return to_return;
}

void count_spans_and_traces(gcs::Client* client) {
    std::unordered_set<std::string> traces;
    std::unordered_set<std::string> spans;
    std::vector<std::future<Counts>> counts_futures;
    std::string trace_struct_bucket = std::string(TRACE_STRUCT_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX);
    for (auto& object_metadata : client ->ListObjects(trace_struct_bucket)) {
        if (!object_metadata) {
            std::cerr << "Error in getting object" << std::endl;
            exit(1);
        }
        counts_futures.push_back(std::async(
            std::launch::async, get_counts_for_object, object_metadata->name(), client));
    }
    Counts summation;
    for (int i=0; i < counts_futures.size(); i++) {
        if (i%20 == 0) {
            std::cout << "i " << i << std::endl;
        }
        Counts new_count = counts_futures[i].get();
        for (auto &trace : new_count.traces) {
            summation.traces.insert(trace);
        }
        for (auto &span : new_count.spans) {
            summation.spans.insert(span);
        }
    }
    std::cout << "num traces: " << summation.traces.size() << std::endl;
    std::cout << "num spans: " << summation.spans.size() << std::endl;
}
