#include "id_index.h"

std::vector<std::string> split_string_by_char(const std::string& str, std::string& ch) {
    std::vector<std::string> tokens;
    std::string reg = "(" + ch + ")+";
    split_regex(tokens, str, boost::regex(reg));
    return tokens;
}

std::vector<std::string> generate_prefixes(time_t earliest, time_t latest) {
    // you want to generate a list of prefixes between earliest and latest
    // find the first digit at which they differ, then do a list on lowest to highest there
    // is this the absolute most efficient?  No, but at a certain point the network calls cost,
    // and I think this is good enough.

    std::vector<std::string> to_return;
    std::stringstream e;
    e << earliest;
    std::stringstream l;
    l << latest;

    std::string e_str = e.str();
    std::string l_str = l.str();

    int i=0;
    for ( ; i<e_str.length(); i++) {
        if (e_str[i] != l_str[i]) {
            break;
        }
    }

    // i is now the first spot of difference
    long min = atol(&e_str[i]);
    long max = atol(&l_str[i]);

    for (int j=min; j<=max; j++) {
        std::string prefix = e_str.substr(0, i);
        prefix += e_str[i];
        to_return.push_back(prefix);
    }
    return to_return;
}

std::vector<std::string> get_batches_between_timestamps(gcs::Client* client, time_t earliest, time_t latest) {
    // TODO
    std::vector<std::string> to_return;
    for (auto&& object_metadata : client->ListObjects(trace_struct_bucket, gcs::Prefix())) {
        if (!object_metadata) {
            throw std::runtime_error(object_metadata.status().message());
        }

    }
    return to_return;
}


bloom_filter create_bloom_filter(gcs::Client* client, std::string batch, time_t earliest, time_t latest) {
    bloom_parameters parameters;

    // How many elements roughly do we expect to insert?
    parameters.projected_element_count = 2500;

    // Maximum tolerable false positive probability? (0,1)
    parameters.false_positive_probability = 0.0001; // 1 in 10000

    parameters.compute_optimal_parameters();
    bloom_filter filter(parameters);
    
    std::string hyphen = "-";
    auto batch_split = split_string_by_char(batch, hyphen);
    if ((time_t) stol(batch_split[1]) > earliest){

    }
    auto reader = client->ReadObject(trace_struct_bucket, batch);
    std::string contents{std::istreambuf_iterator<char>{reader}, {}};
    std::string newline = "\n";
    std::vector<std::string> trace_and_spans = split_string_by_char(contents, newline);
    for (int j=0; j<trace_and_spans.size(); j++) {
        if (trace_and_spans[j].find("Trace ID") != 0) {
            // TODO (1) check if trace lies between timestamps
            std::string trace_id = trace_and_spans[j].substr(trace_and_spans[j].find("Trace ID") + 8 ); // 8 is len of Trace ID
            // (2) insert the trace in
            filter.insert(trace_id);
        }
    }

    return filter;
}

int bubble_up_bloom_filter(gcs::Client* client, bloom_filter bf) {
    // TODO

}



int update_index(gcs::Client* client, time_t last_updated) {
    time_t now;
    time(&now);
    time_t granularity = 1000;
    std::vector<std::string> batches = get_batches_between_timestamps(client, last_updated, now-(now%granularity));
    bloom_filter bf = create_bloom_filter(client, batches[0], 0, 1000);

}

int main(int argc, char* argv[]) {                                              
    // Create a client to communicate with Google Cloud Storage. This client    
    // uses the default configuration for authentication and project id.        
    auto client = gcs::Client();
    time_t last_updated = 0;
    //
    
    return 0;                                                                   
}
