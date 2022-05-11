#include "id_index.h"

using ::google::cloud::StatusOr;

std::vector<std::string> split_string_by_char(std::string& str, const char* ch) {
    std::vector<std::string> tokens;
    std::string ch_str(ch);
    std::string reg = "(" + ch_str + ")+";
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

bool greater_than(time_t first, std::string second) {
    std::stringstream sec_stream;
    sec_stream << second;
    std::string sec_str = sec_stream.str();
    long s = stol(sec_str);
    return first > s;
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

    int i = 0;
    for ( ; i < e_str.length(); i++) {
        if (e_str[i] != l_str[i]) {
            break;
        }
    }

    // i is now the first spot of difference

    int min = std::stoi(e_str.substr(i, 1));
    int max = std::stoi(l_str.substr(i, 1));

    for (int j = min; j <= max; j++) {
        std::string prefix = e_str.substr(0, i);
        prefix += std::to_string(j);
        to_return.push_back(prefix);
    }
    return to_return;
}

std::vector<std::string> get_batches_between_timestamps(gcs::Client* client, time_t earliest, time_t latest) {
    std::vector<std::string> to_return;
    std::vector<std::string> prefixes = generate_prefixes(earliest, latest);
    for (int i = 0; i < prefixes.size(); i++) {
        for (int j = 0; j < 10; j++) {
            for (int k=0; k < 10; k++) {
                std::string new_prefix = std::to_string(i) + std::to_string(j) + "-" + prefixes[i];
                for (auto&& object_metadata : client->ListObjects(trace_struct_bucket, gcs::Prefix(new_prefix))) {
                    if (!object_metadata) {
                        throw std::runtime_error(object_metadata.status().message());
                    }
                    // before we push back, should make sure that it's actually between the bounds
                    std::string name = object_metadata->name();
                    std::vector<std::string> times = split_string_by_char(name, hyphen);
                    // we care about three of these:
                    // if we are neatly between earliest and latest, or if we overlap on one side
                    if (less_than(earliest, times[1]) && less_than(earliest, times[2])) {
                        // we're too far back, already indexed this, ignore
                        continue;
                    } else if (greater_than(latest, times[1]) && greater_than(latest, times[2])) {
                        // we're too far ahead;  we're still in the waiting period for this data
                        continue;
                    } else {
                        to_return.push_back(object_metadata->name());
                    }
                }
            }
        }
    }
    return to_return;
}

int create_index_bucket(gcs::Client* client) {
  google::cloud::StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->CreateBucketForProject(
          trace_struct_bucket, "dynamic-tracing",
          gcs::BucketMetadata()
              .set_location("us-central1")
              .set_storage_class(gcs::storage_class::Regional()));
  if (!bucket_metadata) {
    std::cerr << "Error creating bucket " << trace_struct_bucket
              << ", status=" << bucket_metadata.status() << "\n";
    return 1;
  }
  return 0;
}

int bloom_filter_to_storage(gcs::Client* client, std::string object_name, bloom_filter* bf) {
  gcs::ObjectWriteStream stream =
        client->WriteObject(trace_struct_bucket, object_name);
  bf->Serialize(stream);
  stream.Close();
  StatusOr<gcs::ObjectMetadata> metadata = std::move(stream).metadata();
    if (!metadata) {
      throw std::runtime_error(metadata.status().message());
  }

  // now that it is written, just double check you can re-interpret what you've written
  gcs::ObjectReadStream read_stream = client->ReadObject(trace_struct_bucket, object_name);
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
    parameters.false_positive_probability = 0.0001;  // 1 in 10000

    parameters.compute_optimal_parameters();
    bloom_filter filter(parameters);
    auto batch_split = split_string_by_char(batch, hyphen);
    auto reader = client->ReadObject(trace_struct_bucket, batch);
    if (!reader) {
        std::cerr << "Error reading object: " << reader.status() << "\n";
        throw std::runtime_error("Error reading trace object");
    }
    std::string contents{std::istreambuf_iterator<char>{reader}, {}};
    std::vector<std::string> trace_and_spans = split_string_by_char(contents, newline);
    for (int j=0; j < trace_and_spans.size(); j++) {
        if (trace_and_spans[j].find("Trace ID") != -1) {
            // TODO(jessica) (1) check if trace lies between timestamps
            int start = trace_and_spans[j].find("Trace ID");
            std::string trace_id =
                trace_and_spans[j].substr(start + 8 , trace_and_spans[j].length() - 9);  // 8 is len of Trace ID
            // (2) insert the trace in
            filter.insert(trace_id);
        }
    }

    return filter;
}

int attach_leaves_to_tree(gcs::Client* client, std::vector<std::string> batches, std::vector<bloom_filter> bfs) {
    // when you make a parent, it should union all children
    // so first, create level 1:  union based on branching factor
    // we want to make this relatively balanced, but also we don't have forever to traverse
    // so just do a greedy algorithm of add to shortest branch of tree

    // okay so you can do this in a for loop.  log(10) of batches is how many levels you will have
    for (double i=0; i < log10(batches.size()); i++) {
        // TODO(jessica) create subtree here, then attach it to rest of tree
    }
    return 0;
}



int update_index(gcs::Client* client, time_t last_updated) {
    time_t now;
    time(&now);
    time_t granularity = 1000;
    std::vector<std::string> batches = get_batches_between_timestamps(client, last_updated, now-(now%granularity));
    std::vector<bloom_filter> bloom_filters;
    std::map<std::string, bloom_filter*> batch_to_bf;
    create_index_bucket(client);
    for (int i=0; i < batches.size(); i++) {
        std::cout << "batch " << i << "  " << batches[i] << std::endl;
        bloom_filters.push_back(create_bloom_filter(client, batches[i], 0, 1000));
        bloom_filter_to_storage(client, batches[i], &bloom_filters[bloom_filters.size()-1]);
    }
    attach_leaves_to_tree(client, batches, bloom_filters);
    return 0;
}
