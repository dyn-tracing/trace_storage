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

bool leaf_sizes_equal(struct Leaf &leaf1, struct Leaf &leaf2) {
    if (leaf1.batch_names.size() != leaf2.batch_names.size()) { return false; }
    if (leaf1.bloom_filters.size() != leaf2.bloom_filters.size()) { return false; }
    if (leaf1.bloom_filters.size() != leaf1.batch_names.size()) { return false; }
    if (leaf2.bloom_filters.size() != leaf2.batch_names.size()) { return false; }
    return true;
}

bool batch_names_equal(struct Leaf &leaf1, struct Leaf &leaf2) {
    for (int i=0; i<leaf1.batch_names.size(); i++) {
        if (leaf1.batch_names[i].compare(leaf2.batch_names[i]) != 0) {
            return false;
        }
    }
    return true;
}

bool bloom_filters_equal(struct Leaf &leaf1, struct Leaf &leaf2) {
    for (int i=0; i<leaf1.batch_names.size(); i++) {
        if (leaf1.bloom_filters[i] != leaf2.bloom_filters[i]) {
            return false;
        }
    }
    return true;


}

bool leaf_equals(struct Leaf &leaf1, struct Leaf &leaf2) {
    return leaf_sizes_equal(leaf1, leaf2) && batch_names_equal(leaf1, leaf2) && bloom_filters_equal(leaf1, leaf2);
}

void serialize(struct Leaf leaf, std::ostream &os) {
    // when you serialize names, start with number of batch names
    unsigned int batch_size = leaf.batch_names.size();
    os.write((char *) &batch_size, sizeof(unsigned int));
    for (int i=0; i<leaf.batch_names.size(); i++) {
        unsigned int batch_name_len = leaf.batch_names[i].length();
        os.write((char *) &batch_name_len, sizeof(unsigned int));
        os.write((char *) &leaf.batch_names[i], leaf.batch_names[i].length()*sizeof(char*));
    }
    for (int i=0; i<leaf.bloom_filters.size(); i++) {
        leaf.bloom_filters[i].Serialize(os);
    }
}

struct Leaf deserialize(std::istream &is) {
    Leaf leaf;
    unsigned int batch_size;
    is.read((char *) &batch_size, sizeof(unsigned int));
    for (unsigned int i=0; i<batch_size; i++) {
        std::string batch_name;
        unsigned int batch_name_len;
        is.read((char *) &batch_name_len, sizeof(unsigned int));
        batch_name.reserve(batch_name_len);
        is.read((char *) &batch_name, sizeof(char*) * batch_name_len);
        leaf.batch_names.push_back(batch_name);
    }
    for (unsigned int i=0; i<batch_size; i++) {
        bloom_filter bf;
        bf.Deserialize(is);
        leaf.bloom_filters.push_back(bf);
    }
    return leaf;
}

/* Returns 3 lists:
 * 1. Batches that are wholly within the timestamps earliest and latest
 * 2. Batches that overlap with the earlier part of the batch
 * 3. Batches that overlap with the later part of the batch
 */
std::vector<std::vector<std::string>> get_batches_between_timestamps(gcs::Client* client, time_t earliest, time_t latest) {
    std::vector<std::string> within_range;
    std::vector<std::string> early_in_range;
    std::vector<std::string> late_in_range;
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
                        if (greater_than(earliest, times[1])) {
                            early_in_range.push_back(object_metadata->name());
                        } else if (greater_than(latest, times[2])) {
                            late_in_range.push_back(object_metadata->name());
                        } else {
                            within_range.push_back(object_metadata->name());
                        }
                    }
                }
            }
        }
    }
    std::vector<std::vector<std::string>> to_return;
    to_return.push_back(within_range);
    to_return.push_back(early_in_range);
    to_return.push_back(late_in_range);
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

  /*
  // now that it is written, just double check you can re-interpret what you've written
  gcs::ObjectReadStream read_stream = client->ReadObject(trace_struct_bucket, object_name);
  bloom_filter retrieved_bf;
  retrieved_bf.Deserialize(read_stream);
  read_stream.Close();
  assert(retrieved_bf == *bf);
  */
  return 0;
}

bloom_filter create_bloom_filter_entire_batch(gcs::Client* client, std::string batch) {
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
            int start = trace_and_spans[j].find("Trace ID");
            std::string trace_id =
                trace_and_spans[j].substr(start + 8 , trace_and_spans[j].length() - 9);  // 8 is len of Trace ID
            // (2) insert the trace in
            filter.insert(trace_id);
        }
    }

    return filter;
}


int make_leaf(gcs::Client* client, std::vector<std::vector<std::string>> batches, time_t start_time, time_t end_time) {
    Leaf leaf;
    int number_of_batches = 0;
    // 1. Incorporate entire batches
    for (int i=0; i<batches[0].size(); i++) {
        leaf.batch_names.push_back(batches[0][i]);
        leaf.bloom_filters.push_back(create_bloom_filter_entire_batch(client, batches[0][i]));
    }
    // 2. Incorporate batches 
    for (int j=0; j<batches[1].size(); j++) {

    }
}

int bubble_up_leaf(gcs::Client* client, time_t start_time, time_t end_time) {
    return 0;

}

int update_index(gcs::Client* client, time_t last_updated) {
    time_t now;
    time(&now);
    time_t granularity = 1000;
    // we need to update all batches between last_updated and now-granularity
    for (time_t i=last_updated; i<now-(now%granularity); i+= granularity) {
        // so for each, a batch
        std::vector<std::vector<std::string>> batches = get_batches_between_timestamps(client, i, i+granularity);
        // now, make Bloom filters
        make_leaf(client, batches, i, i+granularity);
        bubble_up_leaf(client, i, i+granularity);

    }
    return 0;
}
