#include "indices/make_sequence_bloom_tree/id_index.h"

using ::google::cloud::StatusOr;

bool leaf_sizes_equal(struct Leaf &leaf1, struct Leaf &leaf2) {
    if (leaf1.batch_names.size() != leaf2.batch_names.size()) { return false; }
    if (leaf1.bloom_filters.size() != leaf2.bloom_filters.size()) { return false; }
    if (leaf1.bloom_filters.size() != leaf1.batch_names.size()) { return false; }
    if (leaf2.bloom_filters.size() != leaf2.batch_names.size()) { return false; }
    return true;
}

bool batch_names_equal(struct Leaf &leaf1, struct Leaf &leaf2) {
    for (uint64_t i=0; i < leaf1.batch_names.size(); i++) {
        if (leaf1.batch_names[i].compare(leaf2.batch_names[i]) != 0) {
            return false;
        }
    }
    return true;
}

bool bloom_filters_equal(struct Leaf &leaf1, struct Leaf &leaf2) {
    for (uint64_t i=0; i < leaf1.batch_names.size(); i++) {
        if (leaf1.bloom_filters[i] != leaf2.bloom_filters[i]) {
            return false;
        }
    }
    return true;
}

bool leaf_equals(struct Leaf &leaf1, struct Leaf &leaf2) {
    return leaf_sizes_equal(leaf1, leaf2) && batch_names_equal(leaf1, leaf2) && bloom_filters_equal(leaf1, leaf2);
}

void serialize_leaf(struct Leaf leaf, std::ostream &os) {
    // when you serialize names, start with number of batch names
    unsigned int batch_size = leaf.batch_names.size();
    os.write(reinterpret_cast<char *>(&batch_size), sizeof(unsigned int));
    for (uint64_t i=0; i < leaf.batch_names.size(); i++) {
        unsigned int batch_name_len = leaf.batch_names[i].length();
        os.write(reinterpret_cast<char *>(&batch_name_len), sizeof(unsigned int));
        os << leaf.batch_names[i];
    }
    for (uint64_t i=0; i < leaf.bloom_filters.size(); i++) {
        leaf.bloom_filters[i].Serialize(os);
    }
}

struct Leaf deserialize_leaf(std::istream &is) {
    Leaf leaf;
    unsigned int batch_size;
    is.read(reinterpret_cast<char *>(&batch_size), sizeof(unsigned int));
    leaf.batch_names.reserve(batch_size);
    leaf.bloom_filters.reserve(batch_size);
    for (unsigned int i=0; i < batch_size; i++) {
        unsigned int batch_name_len;
        is.read(reinterpret_cast<char *>(&batch_name_len), sizeof(unsigned int));
        std::vector<char> tmp(static_cast<int>(batch_name_len));
        is.read(tmp.data(), sizeof(char)*static_cast<int>(batch_name_len));
        std::string batch_name;
        batch_name.assign(tmp.data(), static_cast<int>(batch_name_len));
        leaf.batch_names.push_back(batch_name);
    }
    for (unsigned int i=0; i < batch_size; i++) {
        bloom_filter bf;
        bf.Deserialize(is);
        leaf.bloom_filters.push_back(bf);
    }
    return leaf;
}

/*
  If bucket already exists, returns time last updated.
  Otherwise, returns 0.
*/
time_t create_index_bucket(gcs::Client* client, std::string index_bucket) {
    google::cloud::StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->CreateBucketForProject(
          index_bucket, PROJECT_ID,
          gcs::BucketMetadata()
              .set_location(BUCKETS_LOCATION)
              .set_storage_class(gcs::storage_class::Regional()));
    if (bucket_metadata.status().code() == ::google::cloud::StatusCode::kAborted) {
      // means we've already created the bucket
      std::tuple<time_t, time_t> root;
      time_t granularity;
      get_root_and_granularity(client, root, granularity, index_bucket);
      return std::get<1>(root);
    } else if (!bucket_metadata) {
    std::cerr << "Error creating bucket " << index_bucket
              << ", status=" << bucket_metadata.status() << "\n";
      return -1;
    }
    // set bucket type
    StatusOr<gcs::BucketMetadata> updated_metadata = client->PatchBucket(
      index_bucket,
      gcs::BucketMetadataPatchBuilder().SetLabel("bucket_type", "bloom_index"));

    if (!updated_metadata) {
      throw std::runtime_error(updated_metadata.status().message());
    }
    return 0;
}

std::vector<std::string> trace_ids_from_trace_id_object(gcs::Client* client, std::string obj_name) {
    std::vector<std::string> to_return;
    auto batch_split = split_by_string(obj_name, hyphen);
    std::string trace_struct_bucket(TRACE_STRUCT_BUCKET_PREFIX);
    std::string suffix(BUCKETS_SUFFIX);
    StatusOr<std::string> contents = read_object(trace_struct_bucket+suffix, obj_name, client);
    if (!contents.ok()) {
        std::cerr << "trace_ids_from_trace_id_object: error in getting contents" << std::endl;
        return to_return;
    }

    std::vector<std::string> trace_and_spans = split_by_string(contents.value(), newline);
    for (uint64_t j=0; j < trace_and_spans.size(); j++) {
        if (trace_and_spans[j].find("Trace ID") != std::string::npos) {
            int start = trace_and_spans[j].find("Trace ID");
            std::string trace_id =
                trace_and_spans[j].substr(start + 10, trace_and_spans[j].length() - 11);  // 8 is len of Trace ID
            std::cout << "found trace ID " << trace_id << std::endl;
            to_return.push_back(trace_id);
        }
    }
    return to_return;
}

std::vector<std::string> span_ids_from_trace_id_object(gcs::Client* client, std::string obj_name) {
    std::vector<std::string> to_return;
    auto batch_split = split_by_string(obj_name, hyphen);
    std::string trace_struct_bucket(TRACE_STRUCT_BUCKET_PREFIX);
    std::string suffix(BUCKETS_SUFFIX);
    StatusOr<std::string> contents = read_object(trace_struct_bucket+suffix, obj_name, client);
    if (!contents.ok()) {
        std::cerr << "span_ids_from_trace_id_object: error in getting contents" << std::endl;
        return to_return;
    }

    std::vector<std::string> trace_and_spans = split_by_string(contents.value(), newline);

    for (uint64_t j=0; j < trace_and_spans.size(); j++) {
        if (trace_and_spans[j].find("Trace ID") == std::string::npos && trace_and_spans[j].size() > 0) {
            std::vector<std::string> sp = split_by_string(trace_and_spans[j], colon);
            to_return.push_back(sp[1]);
        }
    }
    return to_return;
}

std::vector<std::string> get_values_in_span_object(gcs::Client* client, std::string bucket_name,
    std::string object_name, property_type prop_type, get_value_func val_func) {
    std::vector<std::string> to_return;

    StatusOr<std::string> contents = read_object(bucket_name, object_name, client);

    if (contents.status().code() == ::google::cloud::StatusCode::kNotFound) {
        // this is fine, just means nothing was put in this microservice for this batch
        return to_return;
    } else if (!contents.ok()) {
        std::cerr << "get_values_in_span_object: Error reading object: " << contents.status() << "\n";
        throw std::runtime_error("Error reading trace object");
    }
    opentelemetry::proto::trace::v1::TracesData tracing_data;
    bool ret = tracing_data.ParseFromString(contents.value());
    if (!ret) {
        throw std::runtime_error("could not parse span data");
    }
    int sp_size = tracing_data.resource_spans(0).scope_spans(0).spans_size();
    for (int i=0; i < sp_size; i++) {
        opentelemetry::proto::trace::v1::Span sp =
            tracing_data.resource_spans(0).scope_spans(0).spans(i);
        to_return.push_back(get_value_as_string(&sp, val_func, prop_type));
    }
    return to_return;
}

std::vector<std::string> values_from_trace_id_object(gcs::Client* client, std::string obj_name,
    std::string property_name, property_type prop_type, get_value_func val_func) {
    std::vector<std::string> to_return;
    // trace ID and span ID are special cases bc you can get them with only the structural object
    if (property_name.compare(TRACE_ID) == 0) {
        return trace_ids_from_trace_id_object(client, obj_name);
    } else if (property_name.compare(SPAN_ID) == 0) {
        return span_ids_from_trace_id_object(client, obj_name);
    }
    // now, retrieve each value from each span
    std::vector<std::string> span_buckets_names = get_spans_buckets_names(client);
    std::vector<std::future<std::vector<std::string>>> future_values;
    for (uint64_t i=0; i < span_buckets_names.size(); i++) {
        future_values.push_back(std::async(std::launch::async, get_values_in_span_object,
            client, span_buckets_names[i], obj_name, prop_type, val_func));
    }
    for (uint64_t i=0; i < future_values.size(); i++) {
        auto new_values = future_values[i].get();
        to_return.insert(to_return.end(), new_values.begin(), new_values.end());
    }
    return to_return;
}

bloom_filter create_bloom_filter_entire_batch(gcs::Client* client, std::string batch,
    std::string property_name, property_type prop_type, get_value_func val_func) {
    std::cout << "creating entire batch " << std::endl;
    bloom_parameters parameters;

    // How many elements roughly do we expect to insert?
    parameters.projected_element_count = element_count;

    // Maximum tolerable false positive probability? (0,1)
    parameters.false_positive_probability = 0.0001;  // 1 in 10000

    parameters.compute_optimal_parameters();
    bloom_filter filter(parameters);
    auto values = values_from_trace_id_object(client, batch, property_name, prop_type, val_func);
    for (uint64_t i=0; i < values.size(); i++) {
        size_t len = values[i].length();
        const char* values_c_str = values[i].c_str();
        filter.insert(values_c_str, len);
    }

    return filter;
}

bloom_filter create_bloom_filter_partial_batch(
    gcs::Client* client, std::string batch, time_t earliest, time_t latest,
    std::string property_name, property_type prop_type, get_value_func val_func
) {
    std::cout << "creating bloom filter partial batch" << std::endl;
    bloom_parameters parameters;

    // How many elements roughly do we expect to insert?
    parameters.projected_element_count = element_count;

    // Maximum tolerable false positive probability? (0,1)
    parameters.false_positive_probability = 0.0001;  // 1 in 10000

    parameters.compute_optimal_parameters();
    bloom_filter filter(parameters);
    auto values_unfiltered = values_from_trace_id_object(client, batch, property_name, prop_type, val_func);
    std::string trace_struct_bucket(TRACE_STRUCT_BUCKET_PREFIX);
    std::string suffix(BUCKETS_SUFFIX);
    StatusOr<std::string> contents = read_object(trace_struct_bucket+suffix, batch, client);
    if (!contents.ok()) {
        std::cerr << "create_bloom_filter_partial_batch: Error reading object: " << contents.status() << "\n";
        throw std::runtime_error("Error reading trace object");
    }
    auto values = filter_trace_ids_based_on_query_timestamp(
        values_unfiltered, batch, contents.value(), earliest, latest, client);

    for (uint64_t i=0; i < values.size(); i++) {
        filter.insert(values[i]);
    }
    return filter;
}


Leaf make_leaf(gcs::Client* client, BatchObjectNames &batch,
    time_t start_time, time_t end_time, std::string index_bucket,
    std::string property_name, property_type prop_type, get_value_func val_func) {
    Leaf leaf;
    leaf.start_time = start_time;
    leaf.end_time = end_time;
    std::vector<std::future<bloom_filter>> inclusive_bloom;
    std::vector<std::future<bloom_filter>> early_bloom;
    std::vector<std::future<bloom_filter>> late_bloom;
    // 1. Incorporate entire batches
    for (uint64_t i=0; i < batch.inclusive.size(); i++) {
        leaf.batch_names.push_back(batch.inclusive[i]);
        inclusive_bloom.push_back(std::async(std::launch::async,
            create_bloom_filter_entire_batch, client, batch.inclusive[i],
            property_name, prop_type, val_func));
    }

    // 2. Incorporate batches that overlap the first part of the time range (ie go shorter than it)
    for (uint64_t j=0; j < batch.early.size(); j++) {
        leaf.batch_names.push_back(batch.early[j]);
        early_bloom.push_back(std::async(std::launch::async,
            create_bloom_filter_partial_batch, client, batch.early[j], start_time, end_time,
            property_name, prop_type, val_func));
    }

    // 3. Incorporate batches that overlap the later part of the time range (ie go longer than it)
    for (uint64_t k=0; k < batch.late.size(); k++) {
        leaf.batch_names.push_back(batch.late[k]);
        late_bloom.push_back(std::async(std::launch::async,
            create_bloom_filter_partial_batch, client, batch.late[k], start_time, end_time,
            property_name, prop_type, val_func));
    }

    // 4. Get all the futures from async calls to be actual values
    for (uint64_t i=0; i < inclusive_bloom.size(); i++) {
        leaf.bloom_filters.push_back(inclusive_bloom[i].get());
    }

    for (uint64_t j=0; j < early_bloom.size(); j++) {
        leaf.bloom_filters.push_back(early_bloom[j].get());
    }

    for (uint64_t k=0; k < late_bloom.size(); k++) {
        leaf.bloom_filters.push_back(late_bloom[k].get());
    }
    // 5. Put that leaf in storage
    std::stringstream objname_stream;
    objname_stream << start_time << "-" << end_time;
    gcs::ObjectWriteStream stream =
            client->WriteObject(index_bucket, objname_stream.str());
    serialize_leaf(leaf, stream);
    stream.Close();
    StatusOr<gcs::ObjectMetadata> metadata = std::move(stream).metadata();
    if (!metadata) {
        throw std::runtime_error(metadata.status().message());
    }

    // gonna double check this`
    auto reader = client->ReadObject(index_bucket, objname_stream.str());
    if (!reader) {
        std::cerr << "Error reading object: " << reader.status() << "\n";
        throw std::runtime_error("Error reading trace object");
    }
    Leaf l2 = deserialize_leaf(reader);
    assert(leaf_equals(l2, leaf));
    return leaf;
}

std::tuple<time_t, time_t> get_parent(time_t start_time, time_t end_time, time_t granularity) {
    time_t difference_for_parent = (end_time-start_time)*granularity;
    // this may look unintuitive, but bc integer division, difference for parent checks out
    time_t parent_start = (start_time/difference_for_parent)*difference_for_parent;
    return std::make_tuple(parent_start, parent_start+difference_for_parent);
}


std::tuple<time_t, time_t>  bubble_up_leaves_helper(gcs::Client* client,
    std::vector<std::tuple<time_t, time_t>> just_modified,
    std::vector<bloom_filter> just_modified_bfs, time_t granularity, std::string index_bucket
) {
    std::map<std::tuple<time_t, time_t>, std::vector<int>> parents;
    for (uint64_t i=0; i < just_modified.size(); i++) {
        // who is my parent?
        auto parent = get_parent(
            std::get<0>(just_modified[i]), std::get<1>(just_modified[i]), granularity);
        parents[parent].push_back(i);
    }
    if (parents.size() < 2) {
        // two possibilities:  we're just propagating up the tree, or we're done here
        // we can tell the difference by whether the parent exists yet and how many children there are
        std::string parent_contents;

        std::tuple<time_t, time_t> to_return;
        for (const auto & [parent, children] : parents) {
            std::tuple<time_t, time_t> parent_bounds = get_parent(std::get<0>(just_modified[children[0]]),
                    std::get<1>(just_modified[children[0]]), granularity);
            std::string parent_object = std::to_string(std::get<0>(parent_bounds))
                + "-" + std::to_string(std::get<1>(parent_bounds));
            auto reader = client->ReadObject(index_bucket, parent_object);
            if (reader.status().code() == ::google::cloud::StatusCode::kNotFound) {
                // parent doesn't exist;  we're done here
                if (children.size() == 1) {
                    return just_modified[children[0]];
                }
            } else if (!reader) {
                std::cerr << "Error reading object " << index_bucket << "/" <<
                    parent_object << " :" << reader.status() << "\n";
                throw std::runtime_error(reader.status().message());
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
            auto ret = bubble_up_leaves_helper(client, new_modified, new_bloom, granularity, index_bucket);
            if (std::get<1>(ret) - std::get<0>(ret) > std::get<1>(to_return) - std::get<0>(to_return)) {
                to_return = ret;
            }
        }
        return to_return;
    }

    std::vector<std::tuple<time_t, time_t>> new_modified;
    std::vector<bloom_filter> new_modified_bfs;
    // normal case:  we have a lot to be writing here
    for (const auto & [parent, children] : parents) {
        bloom_filter unioned_filter = just_modified_bfs[children[0]];
        for (uint64_t i=0; i < children.size(); i++) {
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
    return bubble_up_leaves_helper(client, new_modified, new_modified_bfs, granularity, index_bucket);
}

int bubble_up_leaves(gcs::Client* client, time_t start_time, time_t end_time,
    std::vector<Leaf> &leaves, time_t granularity, std::string index_bucket) {
    // we need to bubble up leaf so that means making a bloom filter that is the union of all of them
    std::vector<std::tuple<time_t, time_t>> newly_modified;
    std::vector<bloom_filter> newly_modified_bfs;
    for (uint64_t i=0; i < leaves.size(); i++) {
        newly_modified.push_back(std::make_tuple(leaves[i].start_time, leaves[i].end_time));
            // bloom filter should be union of all
        if (leaves[i].bloom_filters.size() > 0) {
            bloom_filter unioned_bloom = leaves[i].bloom_filters[0];
            for (uint64_t j=0; j < leaves[i].bloom_filters.size(); j++) {
                unioned_bloom |= leaves[i].bloom_filters[j];
            }
            newly_modified_bfs.push_back(unioned_bloom);
        } else {
            bloom_parameters parameters;

            // How many elements roughly do we expect to insert?
            parameters.projected_element_count = element_count;

            // Maximum tolerable false positive probability? (0,1)
            parameters.false_positive_probability = 0.0001;  // 1 in 10000

            parameters.compute_optimal_parameters();
            bloom_filter empty(parameters);
            newly_modified_bfs.push_back(empty);
        }
    }
    // record the new root in the bucket's metadata
    auto new_root = bubble_up_leaves_helper(client, newly_modified, newly_modified_bfs, granularity, index_bucket);
    std::string root_str = std::to_string(std::get<0>(new_root)) + "-"
            + std::to_string(std::get<1>(new_root));
    StatusOr<gcs::BucketMetadata> updated_metadata = client->PatchBucket(
      index_bucket,
      gcs::BucketMetadataPatchBuilder().SetLabel("root", root_str));

    if (!updated_metadata) {
      throw std::runtime_error(updated_metadata.status().message());
    }
    updated_metadata = client->PatchBucket(
      index_bucket,
      gcs::BucketMetadataPatchBuilder().SetLabel("granularity", std::to_string(granularity)));

    if (!updated_metadata) {
      throw std::runtime_error(updated_metadata.status().message());
    }
    return 0;
}

// List of object names per batch
std::vector<struct BatchObjectNames> split_batches_by_leaf(
    std::vector<std::string> object_names, time_t last_updated, time_t to_update, time_t granularity) {
    int num_leaves = (to_update-last_updated)/granularity;
    std::vector<BatchObjectNames> to_return;

    for (int i=0; i < num_leaves; i++) {
        struct BatchObjectNames new_batch;
        to_return.push_back(new_batch);
    }

    for (uint64_t i=0; i < object_names.size(); i++) {
        std::vector<std::string> timestamps = split_by_string(object_names[i], hyphen);
        // are the timestamps in between a range that I have?
        // to do so, mod it by granularity
        std::stringstream stream;
        stream << timestamps[1];
        std::string str = stream.str();
        time_t start_time = stol(str);

        std::stringstream end_stream;
        end_stream << timestamps[2];
        std::string end_str = end_stream.str();
        time_t end_time = stol(end_str);
        std::cout << "start time is " << start_time << std::endl;
        std::cout << "last updated is " << last_updated << std::endl;
        if (start_time-last_updated < 0) {
            // first one
            to_return[0].early.push_back(object_names[i]);
        } else {
            time_t index_for_start = (start_time-last_updated) / granularity;
            time_t index_for_end = (end_time-last_updated) / granularity;
            if (index_for_start == index_for_end) {
                std::cout << "pushing back inclusive" << std::endl;
                to_return[index_for_start].inclusive.push_back(object_names[i]);
            } else {
                if (static_cast<uint64_t>(index_for_end) < to_return.size()) {
                    std::cout << "pushing back late and early" << std::endl;
                    to_return[index_for_start].late.push_back(object_names[i]);
                    to_return[index_for_end].early.push_back(object_names[i]);
                } else {
                    std::cout << "pushing back here" << std::endl;
                    to_return[index_for_start].late.push_back(object_names[i]);
                }
            }
        }
    }
    return to_return;
}

Status get_root_and_granularity(gcs::Client* client, std::tuple<time_t, time_t> &root,
    time_t &granularity, std::string ib) {
    // get root and granularity from labels
    StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->GetBucketMetadata(ib);
    if (!bucket_metadata) {
        throw std::runtime_error(bucket_metadata.status().message());
        return bucket_metadata.status();
    }
    for (auto const& kv : bucket_metadata->labels()) {
        if (kv.first == "root") {
            std::string root_name = kv.second;
            std::vector<std::string> times = split_by_string(root_name, hyphen);
            root = std::make_tuple(
                time_t_from_string(times[0]),
                time_t_from_string(times[1]));
        }
        if (kv.first == "granularity") {
            granularity = time_t_from_string(kv.second);
        }
    }
    return Status();
}

int update_index(gcs::Client* client, std::string property_name, time_t granularity,
    property_type prop_type, get_value_func val_func) {
    std::string index_bucket = property_name;
    replace_all(index_bucket, ".", "-");
    index_bucket = "index-" + index_bucket + std::string(BUCKETS_SUFFIX);
    time_t now;
    time(&now);
    time_t to_update = now-(now%granularity);
    time_t last_updated = create_index_bucket(client, index_bucket);
    if (last_updated == 0) {
        last_updated = get_lowest_time_val(client);
        last_updated = last_updated - (last_updated%granularity);
    }
    // time_t to_update = last_updated + (20*granularity); // Used for testing

    std::vector<std::string> batches = get_batches_between_timestamps(client, last_updated, to_update);
    std::vector<BatchObjectNames> batches_by_leaf = split_batches_by_leaf(
        batches, last_updated, to_update, granularity);

    int j = 0;
    std::vector<std::future<Leaf>> leaves_future;
    std::vector<Leaf> leaves;
    for (time_t i=last_updated; i < to_update; i+= granularity) {
        leaves_future.push_back(std::async(std::launch::async, make_leaf,
            client, std::ref(batches_by_leaf[j]), i, i+granularity, index_bucket,
            property_name, prop_type, val_func));
        j++;
    }

    for (uint64_t i=0; i < leaves_future.size(); i++) {
        leaves.push_back(leaves_future[i].get());
    }
    bubble_up_leaves(client, last_updated, to_update, leaves, granularity, index_bucket);
    return 0;
}
