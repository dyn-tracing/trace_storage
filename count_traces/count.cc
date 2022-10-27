#include "count_traces/count.h"

#include <unordered_set>
#include <vector>

const char CORRECT_HASH[] = "35917098744";
const char TRACE_BUCKET[] = "dyntraces-snicket4";


struct Counts {
    std::unordered_set<std::string> traces;
    std::unordered_set<std::string> spans;
};

Counts get_counts_for_object(std::string object, gcs::Client* client) {
    Counts to_return;
    auto reader = client->ReadObject(TRACE_BUCKET, object);
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
    for (auto& object_metadata : client ->ListObjects(TRACE_BUCKET)) {
        if (!object_metadata) {
            std::cerr << "Error in getting object" << std::endl;
            exit(1);
        }
        counts_futures.push_back(std::async(
            std::launch::async, get_counts_for_object, object_metadata->name(), client));
    }
    Counts summation;
    for (int64_t i=0; i < counts_futures.size(); i++) {
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
