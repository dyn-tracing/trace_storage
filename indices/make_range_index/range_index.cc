#include "range_index.h"

#include <map>
#include <string>
#include <utility>

StatusOr<time_t> create_index_bucket(gcs::Client* client, std::string index_bucket) {
    google::cloud::StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->CreateBucketForProject(
          index_bucket, PROJECT_ID,
          gcs::BucketMetadata()
              .set_location(BUCKETS_LOCATION)
              .set_storage_class(gcs::storage_class::Regional()));

    if (bucket_metadata.status().code() == ::google::cloud::StatusCode::kAborted) {
      // means we've already created the bucket
      for (auto const & kv : bucket_metadata->labels()) {
          if (kv.first == "last_updated") {
              return time_t_from_string(kv.second);
          }
      }
    } else if (!bucket_metadata) {
        std::cerr << "Error creating bucket " << index_bucket
              << ", status=" << bucket_metadata.status() << "\n";
        return bucket_metadata.status();
    }

    // set bucket type
    StatusOr<gcs::BucketMetadata> updated_metadata = client->PatchBucket(
      index_bucket,
      gcs::BucketMetadataPatchBuilder().SetLabel("bucket_type", "range_index"));

    if (!updated_metadata) {
      std::cerr << "failed to patch metadata for bucket_type" << std::endl;
      throw std::runtime_error(updated_metadata.status().message());
    }
    updated_metadata = client->PatchBucket(
      index_bucket,
      gcs::BucketMetadataPatchBuilder().SetLabel("time_range_per_node",
        std::to_string(TIME_RANGE_PER_NODE)));

    if (!updated_metadata) {
      std::cerr << "failed to patch metadata for time_range_per_node" << std::endl;
      throw std::runtime_error(updated_metadata.status().message());
    }
    updated_metadata = client->PatchBucket(
      index_bucket,
      gcs::BucketMetadataPatchBuilder().SetLabel("nodes_per_summary",
        std::to_string(NUM_NODES_PER_SUMMARY)));

    if (!updated_metadata) {
      std::cerr << "failed to patch metadata for nodes_per_summary" << std::endl;
      throw std::runtime_error(updated_metadata.status().message());
    }
    return 0;
}

std::vector<RawData> retrieve_single_batch_single_bucket_data(
    const std::vector<std::string> batches,
    const int64_t batch_index,
    const std::string bucket_name,
    const std::string attribute_to_index,
    gcs::Client* client) {

    std::vector<RawData> to_return;

    StatusOr<std::string> raw_trace_data = read_object(bucket_name, batches[batch_index], client);
    if (raw_trace_data.status().code() == ::google::cloud::StatusCode::kNotFound) {
        return to_return;
    }
    if (!raw_trace_data.ok()) {
        std::cerr << "raw trace data was not retrieved properly because "
            << raw_trace_data.status().message() << std::endl;
    }
    if (raw_trace_data.value().length() < 1) { return to_return; }

    ot::TracesData trace_data;
    bool ret = trace_data.ParseFromString(raw_trace_data.value());
    if (ret == false) {
        std::cerr << "failed to parse trace data" << std::endl;
        return to_return;
    }

    const opentelemetry::proto::trace::v1::Span* sp;

    for (int i=0;
        i < trace_data.resource_spans(0).scope_spans(0).spans_size();
        i++) {
        sp = &(trace_data.resource_spans(0).scope_spans(0).spans(i));
        std::string trace_id = hex_str(sp->trace_id(), sp->trace_id().length());
        // dividing into seconds
        time_t timestamp = sp->start_time_unix_nano() / 1000000000;
        if (attribute_to_index.compare("latency") == 0) {
            to_return.push_back(
                RawData {
                    .batch_index = batch_index,
                    .timestamp = timestamp,
                    .trace_id = trace_id,
                    .data = std::to_string(sp->end_time_unix_nano() - sp->start_time_unix_nano()),
                });
        } else {
            const opentelemetry::proto::common::v1::KeyValue* attribute;
            for (int j=0; j < sp->attributes_size(); j++) {
                attribute =  &(sp->attributes(j));
                const opentelemetry::proto::common::v1::AnyValue* val =
                    &(attribute->value());
                auto curr_attr_key = attribute->key();
                std::string curr_attr_val = "";
                switch (val->value_case()) {
                case 1:
                    curr_attr_val = val->string_value();
                    break;
                case 2:
                    curr_attr_val = val->bool_value() ? "true" : "false";
                    break;
                case 3:
                    curr_attr_val = std::to_string(val->int_value());
                    break;
                case 4:
                    curr_attr_val = std::to_string(val->double_value());
                    break;
                default:
                    /*
                    return Status(
                        ::google::cloud::StatusCode::kUnavailable,
                        "could not translate attribute");
                    */
                    break;
                }
                if (attribute_to_index == curr_attr_key) {
                    to_return.push_back(
                        RawData {
                            .batch_index = batch_index,
                            .timestamp = timestamp,
                            .trace_id = trace_id,
                            .data = curr_attr_val,
                        });
                }
            }
        }
    }
    return to_return;
}

StatusOr<std::vector<RawData>> retrieve_single_batch_data(
    const std::vector<std::string> batches,
    const int64_t batch_index,
    const std::string attribute_to_index,
    gcs::Client* client) {
    std::vector<RawData> to_return;
    std::vector<std::string> span_buckets_names = get_spans_buckets_names(client);
    std::vector<std::future<std::vector<RawData>>> raw_data_futures;

    for (int64_t i=0; i < span_buckets_names.size(); i++) {
        raw_data_futures.push_back(std::async(std::launch::async,
            retrieve_single_batch_single_bucket_data,
            batches, batch_index, span_buckets_names[i],
            attribute_to_index, client));
    }

    for (int64_t i=0; i < raw_data_futures.size(); i++) {
        std::vector<RawData> data = raw_data_futures[i].get();
        to_return.insert(to_return.end(), data.begin(), data.end());
    }

    return to_return;
}

Status organize_data_into_nodes(
    const std::vector<std::string> &batches,
    const std::string attribute_to_index,
    std::map<time_t, Node>* nodes,
    gcs::Client* client) {

    std::vector<std::future<StatusOr<std::vector<RawData>>>> data_futures;
    data_futures.reserve(batches.size());
    for (int64_t i=0; i < batches.size(); i++) {
        data_futures.push_back(std::async(std::launch::async,
            retrieve_single_batch_data,
            batches, i, attribute_to_index, client));
    }

    time_t has_contents = 0;

    for (int64_t i=0; i < data_futures.size(); i++) {
        StatusOr<std::vector<RawData>> partial_data = data_futures[i].get();
        if (!partial_data.ok()) {
            std::cerr << "partial data failed" << std::endl;
            return partial_data.status();
        }
        // for each piece of raw data, integrate into Nodes, but all in one large NodePieces
        for (int64_t j = 0; j < partial_data->size(); j++) {
            RawData* cur_data = &partial_data->at(j);
            time_t start_batch_time = cur_data->timestamp -
                (cur_data->timestamp % TIME_RANGE_PER_NODE);
            has_contents = start_batch_time;
            nodes->at(start_batch_time).data.push_back(IndexedData {
                .batch_name = batches[cur_data->batch_index],
                .trace_id = cur_data->trace_id,
                .data = cur_data->data
            });
        }
    }
    return Status();
}

Status write_node_to_storage(Node& node, std::string node_name,
                            std::string bucket_name,
                            gcs::Client* client) {
    gcs::ObjectWriteStream stream = client->WriteObject(bucket_name, node_name);
    node.Serialize(stream);
    stream.Close();
    StatusOr<gcs::ObjectMetadata> metadata = std::move(stream).metadata();
    if (!metadata) {
        throw std::runtime_error(metadata.status().message());
    }
    return Status();
}

Status update_summary_object(const std::map<time_t, Node> &nodes,
                            const time_t start_time, const time_t end_time,
                            std::string index_bucket, gcs::Client* client) {
    std::string summary_object_name = "summary-" +
        std::to_string(start_time) + "-" +
        std::to_string(start_time+(TIME_RANGE_PER_NODE*NUM_NODES_PER_SUMMARY));

    // first, read object
    auto reader = client->ReadObject(index_bucket, summary_object_name);
    if (!reader) {
        return reader.status();
    }
    NodeSummary sum;
    Status ret = sum.Deserialize(reader);
    if (!ret.ok()) {
        return ret;
    }

    // Now update sum from last to now.
    time_t soonest_time = std::get<0>(
        sum.node_objects[sum.node_objects.size()-1]) + TIME_RANGE_PER_NODE;

    for (int64_t i=soonest_time; i < end_time; i+=TIME_RANGE_PER_NODE) {
        std::vector<Node> nodes_to_write = nodes.at(i).Split();

        // For each node, their contents are sorted. Put
        // pointers to them in the NodeSummary object, and put them in storage.
        for (int64_t j=0; j < nodes_to_write.size(); j++) {
            sum.node_objects.push_back(std::make_pair(
                nodes_to_write[j].start_time,
                nodes_to_write[j].data[0].data));
            // Now, send that object to storage.
            std::string node_name =
                std::to_string(nodes_to_write[j].start_time) +
                std::to_string(nodes_to_write[j].end_time) +
                nodes_to_write[j].data[0].data;
            ret = write_node_to_storage(nodes_to_write[j], node_name,
                index_bucket, client);
            if (!ret.ok()) {
                return ret;
            }
        }
    }
    return Status();
}

Status create_summary_object(const std::map<time_t, Node> &nodes,
                             const time_t start_time,
                             const time_t end_time,
                             std::string index_bucket,
                             gcs::Client* client) {
    Status ret;
    NodeSummary sum = {
        .start_time = start_time,
        .end_time = end_time
    };
    for (int64_t i=start_time; i < end_time; i+=TIME_RANGE_PER_NODE) {
        std::vector<Node> nodes_to_write = nodes.at(i).Split();
        // For each node, their contents are sorted. Put
        // pointers to them in the NodeSummary object, and put them in storage.
        for (int64_t j=0; j < nodes_to_write.size(); j++) {
            if (nodes_to_write[j].data.size() > 0) {
                sum.node_objects.push_back(std::make_pair(
                    nodes_to_write[j].start_time,
                    nodes_to_write[j].data[0].data));
                // Now, send that object to storage.
                std::string node_name =
                    std::to_string(nodes_to_write[j].start_time) + "-" +
                    std::to_string(nodes_to_write[j].end_time) + "-" +
                    nodes_to_write[j].data[0].data;
                ret = write_node_to_storage(nodes_to_write[j], node_name,
                    index_bucket, client);
                if (!ret.ok()) {
                    std::cerr << "could not write node to storage" << std::endl;
                    return ret;
                }
            }
        }
    }

    // Now, write the summary object.
    gcs::ObjectWriteStream stream = client->WriteObject(index_bucket,
        "summary-"+std::to_string(sum.start_time) + "-" +
        std::to_string(sum.end_time));

    sum.Serialize(stream);
    stream.Close();
    StatusOr<gcs::ObjectMetadata> metadata = std::move(stream).metadata();
    if (!metadata) {
        throw std::runtime_error(metadata.status().message());
    }
    return Status();
}

Status send_index_to_gcs(const std::map<time_t, Node> &nodes,
    const time_t last_updated,
    const time_t now, std::string index_bucket, gcs::Client* client) {
    time_t summary_time = NUM_NODES_PER_SUMMARY * TIME_RANGE_PER_NODE;
    time_t starting_summary_obj_time = last_updated;
    Status ret;

    // If we are updating an existing summary object...
    if (last_updated % summary_time != 0) {
        time_t start_time_incomplete_object = last_updated -
            (last_updated % summary_time);

        // end time is either end of this object, or now, if we haven't
        // gotten that far yet.
        time_t end_time_incomplete_object = start_time_incomplete_object +
            summary_time;
        if (end_time_incomplete_object > now) {
            end_time_incomplete_object = now;
        }
        ret = update_summary_object(nodes, start_time_incomplete_object,
            end_time_incomplete_object, index_bucket, client);
        if (!ret.ok()) {
            return ret;
        }
        starting_summary_obj_time = start_time_incomplete_object + summary_time;
    }

    // For all summary objects that we are creating in full...
    for (time_t i = starting_summary_obj_time;
         i < now - (now % summary_time); i+=summary_time) {
        ret = create_summary_object(nodes, i,
            i+summary_time, index_bucket, client);
        if (!ret.ok()) {
            return ret;
        }
    }

    // For all incomplete summary objects we create...
    if (now % summary_time != 0) {
        ret = create_summary_object(nodes, now - (now % summary_time),
            now, index_bucket, client);
        if (!ret.ok()) {
            return ret;
        }
    }
    return Status();
}

Status update(std::string indexed_attribute, gcs::Client* client) {
    std::string index_bucket = indexed_attribute + "-range-index";
    replace_all(index_bucket, ".", "-");

    // 1. Find granularity and last updated
    time_t last_updated;
    Status ret;
    StatusOr<time_t> last_updated_or = create_index_bucket(client, index_bucket);
    if (!last_updated_or.ok()) {
        return last_updated_or.status();
    }
    last_updated = last_updated_or.value();
    if (last_updated == 0) {
        last_updated = get_lowest_time_val(client);
        last_updated = last_updated -
            (last_updated % (TIME_RANGE_PER_NODE * NUM_NODES_PER_SUMMARY));
    }

    // 2. Create nodes to be added.
    std::map<time_t, Node> nodes;
    time_t now;
    time(&now);
    now = now - (now % TIME_RANGE_PER_NODE);
    // set now to more realistic value TODO: get rid of this
    now = last_updated + 1000;

    for (time_t i = last_updated; i < now; i += TIME_RANGE_PER_NODE) {
       nodes[i] = Node{
        .start_time = i,
        .end_time = i + TIME_RANGE_PER_NODE,
       };
    }

    // 3. Find all objects that have happened since last updated
    // We need to update from now - time range per node left
    std::vector<std::string> batches = get_batches_between_timestamps(
        client, last_updated, now);


    // 3. Organize their data into summary and node objects
    //    To do so, we need to know (1) batch name, (2) timestamp,
    //    (3) trace ID, (4) data
    ret = organize_data_into_nodes(batches, indexed_attribute, &nodes, client);
    if (!ret.ok()) {
        std::cerr << "Could not organize data into nodes" << std::endl;
        return ret;
    }

    // Now, split all nodes that need to be split into 1 GB increments, and
    // send to GCS.
    ret = send_index_to_gcs(nodes, last_updated, now, index_bucket,
        client);
    if (!ret.ok()) {
        return ret;
    }

    // Now update last_updated, since we just updated the index.
    StatusOr<gcs::BucketMetadata> updated_metadata = client->PatchBucket(
      index_bucket,
      gcs::BucketMetadataPatchBuilder().SetLabel("last_updated", std::to_string(now)));
    if (!updated_metadata.ok()) {
        return updated_metadata.status();
    }
    return Status();
}
