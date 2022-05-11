#include "id_index.h"
using ::google::cloud::StatusOr;
std::string trace_id_bucket = "traceidindex-snicket4";
std::string hyphen = "-";
std::vector<std::string> split_string_by_char(std::string& str, std::string& ch) {
    std::vector<std::string> tokens;
    std::string reg = "(" + ch + ")+";
    split_regex(tokens, str, boost::regex(reg));
    return tokens;
}

bool less_than(time_t first, std::string second) {
    std::stringstream sec_stream;
    sec_stream << second;
    std::string sec_str = sec_stream.str();
    long s = stol(sec_str);
    return first < s;
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
    std::vector<std::string> to_return;
    std::vector<std::string> prefixes = generate_prefixes(earliest, latest);
    for (int i=0; i<prefixes.size(); i++) {
        for (int j=0; j<10; j++) {
            for (int k=0; k<10; k++) {
                std::string new_prefix = std::to_string(i) + std::to_string(j) + "-" + prefixes[i];
                for (auto&& object_metadata : client->ListObjects(trace_struct_bucket, gcs::Prefix(new_prefix))) {
                    if (!object_metadata) {
                        throw std::runtime_error(object_metadata.status().message());
                    }
                    // before we push back, should make sure that it's actually between the bounds
                    std::string name = object_metadata->name();
                    std::vector<std::string> times = split_string_by_char(name, hyphen);
                    
                    to_return.push_back(object_metadata->name());
                }
            }
        }
    }
    return to_return;
}

int bloom_filter_to_storage(gcs::Client* client, std::string object_name, bloom_filter* bf) {
  /*
  google::cloud::StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->CreateBucketForProject(
          trace_id_bucket, "dynamic-tracing",
          gcs::BucketMetadata()
              .set_location("us-central1")
              .set_storage_class(gcs::storage_class::Regional()));
  if (!bucket_metadata) {
    std::cerr << "Error creating bucket " << trace_id_bucket
              << ", status=" << bucket_metadata.status() << "\n";
    return 1;
  }
  */
  std::cout << "gonna write" << std::endl;
  gcs::ObjectWriteStream stream =
        client->WriteObject(trace_id_bucket, object_name);
  std::cout << "writing" << std::endl;
  bf->Serialize(stream);
  std::cout << "done writing" << std::endl;
  stream.Close();
  std::cout << "closed" << std::endl;
  StatusOr<gcs::ObjectMetadata> metadata = std::move(stream).metadata();
    if (!metadata) {
      throw std::runtime_error(metadata.status().message());
  }

  // now that it is written, just double check you can re-interpret what you've written
  gcs::ObjectReadStream read_stream = client->ReadObject(trace_id_bucket, object_name);
  bloom_filter retrieved_bf;
  retrieved_bf.Deserialize(read_stream);
  read_stream.Close();
  assert(retrieved_bf == *bf);
  std::cout << "can retrieve succcessfully" << std::endl;
  return 0;




}

bloom_filter create_bloom_filter(gcs::Client* client, std::string batch, time_t earliest, time_t latest) {
    bloom_parameters parameters;

    // How many elements roughly do we expect to insert?
    parameters.projected_element_count = 2500;

    // Maximum tolerable false positive probability? (0,1)
    parameters.false_positive_probability = 0.0001; // 1 in 10000

    parameters.compute_optimal_parameters();
    bloom_filter filter(parameters);
    
    auto batch_split = split_string_by_char(batch, hyphen);
    auto reader = client->ReadObject(trace_struct_bucket, batch);
    if (!reader) {
        std::cerr << "Error reading object: " << reader.status() << "\n";
        throw std::runtime_error("Error reading trace object");
    }
    std::string contents{std::istreambuf_iterator<char>{reader}, {}};
    std::string newline = "\n";
    std::vector<std::string> trace_and_spans = split_string_by_char(contents, newline);
    assert(trace_and_spans.size() > 0 );
    for (int j=0; j<trace_and_spans.size(); j++) {
        if (trace_and_spans[j].find("Trace ID") != -1) {
            // TODO (1) check if trace lies between timestamps
            int start = trace_and_spans[j].find("Trace ID");
            std::string trace_id = trace_and_spans[j].substr(start + 8 , trace_and_spans[j].length() - 9); // 8 is len of Trace ID
            // (2) insert the trace in
            filter.insert(trace_id);
        }
    }

    return filter;
}

int bubble_up_bloom_filter(gcs::Client* client, bloom_filter bf) {
    // TODO
    return 0;
}



int update_index(gcs::Client* client, time_t last_updated) {
    time_t now;
    time(&now);
    time_t granularity = 1000;
    //std::vector<std::string> batches = get_batches_between_timestamps(client, last_updated, now-(now%granularity));
    bloom_filter bf = create_bloom_filter(client, "10-1651700436-1651700437", 0, 1000);
    bloom_filter_to_storage(client, "new_bloom", &bf);
    // put into storage
    return 0;

}
