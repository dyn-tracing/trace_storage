#include "id_index/id_index.h"

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
        std::cout << "prefixes " << prefix << std::endl;
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

std::vector<std::string> get_list_result(gcs::Client* client, std::string prefix, time_t earliest, time_t latest) {
    std::vector<std::string> to_return;
    for (auto&& object_metadata : client->ListObjects(trace_struct_bucket, gcs::Prefix(prefix))) {
        if (!object_metadata) {
            throw std::runtime_error(object_metadata.status().message());
        }
        // before we push back, should make sure that it's actually between the bounds
        std::string name = object_metadata->name();
        to_return.push_back(name);
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
            to_return.push_back(name);
        }
    }
    return to_return;
}

std::vector<std::string> get_batches_between_timestamps(gcs::Client* client, time_t earliest, time_t latest) {
    std::vector<std::string> prefixes = generate_prefixes(earliest, latest);
    std::cout << "len of prefixes " << prefixes.size() << std::endl;
    std::vector<std::future<std::vector<std::string>>> object_names;
    for (int i = 0; i < prefixes.size(); i++) {
        for (int j = 0; j < 10; j++) {
            for (int k=0; k < 10; k++) {
                std::string new_prefix = std::to_string(j) + std::to_string(k) + "-" + prefixes[i];
                object_names.push_back(
                    std::async(std::launch::async, get_list_result, client, new_prefix, earliest, latest));
            }
        }
    }
    std::cout << "pushed all async out " << std::endl << std::flush;
    std::vector<std::string> to_return;
    for (int m=0; m<object_names.size(); m++) {
        auto names = object_names[m].get();
        for (int n=0; n<names.size(); n++) {
            // check that these are actually within range
            std::vector<std::string> timestamps = split_string_by_char(names[n], hyphen);
            std::stringstream stream;
            stream << timestamps[1];
            std::string str = stream.str();
            long start_time = stol(str);

            std::stringstream end_stream;
            end_stream << timestamps[2];
            std::string end_str = end_stream.str();
            long end_time = stol(end_str);

            if ((start_time >= earliest && end_time <= latest) ||
                (start_time <= earliest && end_time >= earliest) ||
                (start_time <= latest && end_time >= latest)
            ) {
                to_return.push_back(names[n]);
            }
        }
    }
    std::cout << "returning batch" << std::endl;
    return to_return;
}

int create_index_bucket(gcs::Client* client) {
  google::cloud::StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->CreateBucketForProject(
          index_bucket, "dynamic-tracing",
          gcs::BucketMetadata()
              .set_location("us-central1")
              .set_storage_class(gcs::storage_class::Regional()));
  if (bucket_metadata.status().code() == ::google::cloud::StatusCode::kAborted) {
    // ignore this, means we've already created the bucket
  } else if (!bucket_metadata) {
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


std::vector<std::string> trace_ids_from_trace_id_object(gcs::Client* client, std::string obj_name) {
    std::vector<std::string> to_return;
    auto batch_split = split_string_by_char(obj_name, hyphen);
    auto reader = client->ReadObject(trace_struct_bucket, obj_name);
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
            to_return.push_back(trace_id);
        }
    }
    return to_return;
}

bloom_filter create_bloom_filter_entire_batch(gcs::Client* client, std::string batch) {
    bloom_parameters parameters;

    // How many elements roughly do we expect to insert?
    parameters.projected_element_count = 2500;

    // Maximum tolerable false positive probability? (0,1)
    parameters.false_positive_probability = 0.0001;  // 1 in 10000

    parameters.compute_optimal_parameters();
    bloom_filter filter(parameters);
    auto trace_ids = trace_ids_from_trace_id_object(client, batch);
    for (int i=0; i<trace_ids.size(); i++) {
        filter.insert(trace_ids[i]);
    }

    return filter;
}

bloom_filter create_bloom_filter_partial_batch(gcs::Client* client, std::string batch, time_t earliest, time_t latest) {
    bloom_parameters parameters;

    // How many elements roughly do we expect to insert?
    parameters.projected_element_count = 2500;

    // Maximum tolerable false positive probability? (0,1)
    parameters.false_positive_probability = 0.0001;  // 1 in 10000

    parameters.compute_optimal_parameters();
    bloom_filter filter(parameters);
    auto trace_ids_unfiltered = trace_ids_from_trace_id_object(client, batch);
    auto reader = client->ReadObject(trace_struct_bucket, batch);
    if (!reader) {
        std::cerr << "Error reading object: " << reader.status() << "\n";
        throw std::runtime_error("Error reading trace object");
    }
    std::string contents{std::istreambuf_iterator<char>{reader}, {}};
    auto trace_ids = filter_trace_ids_based_on_query_timestamp(trace_ids_unfiltered, batch, contents, earliest, latest, client);

    for (int i=0; i<trace_ids.size(); i++) {
        filter.insert(trace_ids[i]);
    }
    return filter;
}


Leaf make_leaf(gcs::Client* client, struct BatchObjectNames batch, time_t start_time, time_t end_time) {
    Leaf leaf;
    leaf.start_time = start_time;
    leaf.end_time = end_time;
    std::vector<std::future<bloom_filter>> inclusive_bloom;
    std::vector<std::future<bloom_filter>> early_bloom;
    std::vector<std::future<bloom_filter>> late_bloom;
    // 1. Incorporate entire batches
    for (int i=0; i<batch.inclusive.size(); i++) {
        leaf.batch_names.push_back(batch.inclusive[i]);
        inclusive_bloom.push_back(std::async(std::launch::async, create_bloom_filter_entire_batch, client, batch.inclusive[i]));
    }

    // 2. Incorporate batches that overlap the first part of the time range (ie go shorter than it)
    for (int j=0; j<batch.early.size(); j++) {
        leaf.batch_names.push_back(batch.early[j]);
        leaf.bloom_filters.push_back(create_bloom_filter_partial_batch(client, batch.early[j], start_time, end_time));
        early_bloom.push_back(std::async(std::launch::async, create_bloom_filter_partial_batch, client, batch.early[j], start_time, end_time));
    }

    // 3. Incorporate batches that overlap the later part of the time range (ie go longer than it)
    for (int k=0; k<batch.late.size(); k++) {
        leaf.batch_names.push_back(batch.late[k]);
        leaf.bloom_filters.push_back(create_bloom_filter_partial_batch(client, batch.late[k], start_time, end_time));
        late_bloom.push_back(std::async(std::launch::async, create_bloom_filter_partial_batch, client, batch.late[k], start_time, end_time));
    }

    // 4. Get all the futures from async calls to be actual values
    for (int i=0; i<inclusive_bloom.size(); i++) {
        leaf.bloom_filters.push_back(inclusive_bloom[i].get());
    }

    for (int j=0; j<early_bloom.size(); j++) {
        leaf.bloom_filters.push_back(early_bloom[j].get());
    }

    for (int k=0; k<late_bloom.size(); k++) {
        leaf.bloom_filters.push_back(late_bloom[k].get());
    }
    // 5. Put that leaf in storage
    std::stringstream objname_stream;
    objname_stream << start_time << "-" << end_time;
    gcs::ObjectWriteStream stream =
            client->WriteObject(index_bucket, objname_stream.str());
    serialize(leaf, stream);
    stream.Close();
    StatusOr<gcs::ObjectMetadata> metadata = std::move(stream).metadata();
    if (!metadata) {
        throw std::runtime_error(metadata.status().message());
    }
    return leaf;
}

std::tuple<time_t, time_t> get_parent(time_t start_time, time_t end_time, time_t granularity) {
    time_t difference_for_parent = (end_time-start_time)*granularity;
    // this may look unintuitive, but bc integer division, difference for parent checks out
    time_t parent_start = (start_time/difference_for_parent)*difference_for_parent;
    return std::make_tuple(parent_start, parent_start+difference_for_parent);
}


int bubble_up_leaves_helper(gcs::Client* client,
    std::vector<std::tuple<time_t, time_t>> just_modified,
    std::vector<bloom_filter> just_modified_bfs, time_t granularity
) {
    std::map<std::tuple<time_t, time_t>, std::vector<int>> parents;
    for (int i=0; i < just_modified.size(); i++) {
        // who is my parent?
        auto parent = get_parent(std::get<0>(just_modified[i]), std::get<1>(just_modified[i]), granularity);
        parents[parent].push_back(i);
    }
    if (parents.size() < 2) {
        // two possibilities:  we're just propagating up the tree, or we're done here
        // we can tell the difference by whether the parent exists yet
        std::string parent_contents;

        for (const auto & [parent, children] : parents) {
            std::tuple<time_t, time_t> parent_bounds = get_parent(std::get<0>(just_modified[children[0]]),
                    std::get<1>(just_modified[children[0]]), granularity);
            std::string parent_object = std::to_string(std::get<0>(parent_bounds)) + "-" + std::to_string(std::get<1>(parent_bounds));
            auto reader = client->ReadObject(index_bucket, parent_object);
            if (reader.status().code() == ::google::cloud::StatusCode::kNotFound) {
                // parent doesn't exist;  we're done here
                return 0;
            } else if (!reader) {
                std::cerr << "Error reading object " << index_bucket << "/" << parent_object << " :" << reader.status() << "\n";
                return 1;
            }
            // now we know parent exists, which means we need to keep propagating up
            bloom_filter parental_bloom_filter;
            parental_bloom_filter.Deserialize(reader);
            reader.Close();
            parental_bloom_filter|=just_modified_bfs[0];
            // now rewrite the parental one back out
            gcs::ObjectWriteStream stream =
            client->WriteObject(index_bucket, parent_object);
            parental_bloom_filter.Serialize(stream);
            stream.Close();
            StatusOr<gcs::ObjectMetadata> metadata = std::move(stream).metadata();
            if (!metadata) {
              throw std::runtime_error(metadata.status().message());
            }
            std::vector<std::tuple<time_t, time_t>> new_modified;
            new_modified.push_back(parent_bounds);
            std::vector<bloom_filter> new_bloom;
            new_bloom.push_back(parental_bloom_filter);
            return bubble_up_leaves_helper(client, new_modified, new_bloom, granularity);
        }
    }

    std::vector<std::tuple<time_t, time_t>> new_modified;
    std::vector<bloom_filter> new_modified_bfs;
    // normal case:  we have a lot to be writing here
    for (const auto & [parent, children] : parents){
        bloom_filter unioned_filter = just_modified_bfs[children[0]];
        for (int i=0; i<children.size(); i++) {
            unioned_filter |= just_modified_bfs[children[i]];
        }
        // now write parent
        std::tuple<time_t, time_t> parent_bounds = get_parent(std::get<0>(just_modified[children[0]]),
                std::get<1>(just_modified[children[0]]), granularity);
        std::string parent_object = std::to_string(std::get<0>(parent_bounds)) + "-"
            + std::to_string(std::get<1>(parent_bounds));
        gcs::ObjectWriteStream stream = client->WriteObject(index_bucket, parent_object);
        unioned_filter.Serialize(stream);
        stream.Close();
        StatusOr<gcs::ObjectMetadata> metadata = std::move(stream).metadata();
        if (!metadata) {
          throw std::runtime_error(metadata.status().message());
        }
        new_modified.push_back(parent_bounds);
        new_modified_bfs.push_back(unioned_filter);
    }
    return bubble_up_leaves_helper(client, new_modified, new_modified_bfs, granularity);
}

int bubble_up_leaves(gcs::Client* client, time_t start_time, time_t end_time, std::vector<Leaf> &leaves, time_t granularity) {
    // we need to bubble up leaf so that means making a bloom filter that is the union of all of them
    std::vector<std::tuple<time_t, time_t>> newly_modified;
    std::vector<bloom_filter> newly_modified_bfs;
    for (int i=0; i<leaves.size(); i++) {
        newly_modified.push_back(std::make_tuple(leaves[i].start_time, leaves[i].end_time));
        // bloom filter should be union of all
        bloom_filter unioned_bloom = leaves[i].bloom_filters[0];
        for (int j=0; j<leaves[i].bloom_filters.size(); j++) {
            unioned_bloom |= leaves[i].bloom_filters[j];
        }
        newly_modified_bfs.push_back(unioned_bloom);
    }
    return bubble_up_leaves_helper(client, newly_modified, newly_modified_bfs, granularity);
}

// List of object names per batch
std::vector<struct BatchObjectNames> split_batches_by_leaf(std::vector<std::string> object_names, time_t last_updated, time_t to_update, time_t granularity) {
    int num_leaves = (to_update-last_updated)/granularity;
    std::vector<BatchObjectNames> to_return;

    for (int i=0; i<num_leaves; i++) {
        struct BatchObjectNames new_batch;
        to_return.push_back(new_batch);
    }

    for (int i=0; i<object_names.size(); i++) {
        std::vector<std::string> timestamps = split_string_by_char(object_names[i], hyphen);
        // are the timestamps in between a range that I have?
        // to do so, mod it by granularity
        std::stringstream stream;
        stream << timestamps[1];
        std::string str = stream.str();
        long start_time = stol(str);

        std::stringstream end_stream;
        end_stream << timestamps[2];
        std::string end_str = end_stream.str();
        long end_time = stol(end_str);
        if (start_time-last_updated < 0) {
            // first one
            to_return[0].early.push_back(object_names[i]);
        } else {
            long index_for_start = (start_time-last_updated) / granularity;
            long index_for_end = (end_time-last_updated) / granularity;
            if (index_for_start == index_for_end) {
                to_return[index_for_start].inclusive.push_back(object_names[i]);
            } else {
                if (index_for_end < to_return.size()) {
                    to_return[index_for_start].late.push_back(object_names[i]);
                    to_return[index_for_end].early.push_back(object_names[i]);
                } else {
                    to_return[index_for_start].late.push_back(object_names[i]);
                }
            }
            
        }

    }
    return to_return;
}

int update_index(gcs::Client* client, time_t last_updated) {
    time_t now;
    time(&now);
    time_t granularity = 50;
    //time_t to_update = now-(now%granularity); // this is the right thing;  given I just want to write a little, I override
    time_t to_update = last_updated + (3*granularity);
    create_index_bucket(client);
    std::vector<std::string> batches = get_batches_between_timestamps(client, last_updated, to_update);
    auto batches_by_leaf = split_batches_by_leaf(batches, last_updated, to_update, granularity);
    std::cout << "len batches by leaf " << batches_by_leaf.size() << std::endl;

    int j=0;
    std::vector<Leaf> leaves;
    for (time_t i=last_updated; i<to_update; i+= granularity) {
        std::cout << "in main loop, i is " << i << std::endl;
        leaves.push_back(make_leaf(client, batches_by_leaf[j], i, i+granularity));
        std::cout << "pushed back leaf" << std::endl;
        j++;
    }
    bubble_up_leaves(client, last_updated, to_update, leaves, granularity);
    return 0;
}
