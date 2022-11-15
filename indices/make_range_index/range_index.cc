#include "range_index.h"

#include <map>
#include <string>
#include <utility>

Status get_last_updated_and_granularity(
    gcs::Client* client, time_t &granularity, time_t &last_updated,
    std::string index_bucket
) {
    StatusOr<gcs::BucketMetadata> bucket_metadata =
      client->GetBucketMetadata(index_bucket);
    if (!bucket_metadata) {
        throw std::runtime_error(bucket_metadata.status().message());
        return bucket_metadata.status();
    }
    for (auto const& kv : bucket_metadata->labels()) {
        if (kv.first == "last_updated") {
            last_updated = time_t_from_string(kv.second);
        }
        if (kv.first == "granularity") {
            granularity = time_t_from_string(kv.second);
        }
    }
    return Status();
}

StatusOr<std::vector<RawData>> retrieve_single_batch_data(
    const std::vector<std::string> batches,
    const int64_t batch_index,
    const std::string attribute_to_index,
    gcs::Client* client) {
    std::vector<RawData> to_return;
    ot::TracesData trace_data = read_object_and_parse_traces_data(
        TRACE_STRUCT_BUCKET, batches[batch_index], client);
    const opentelemetry::proto::trace::v1::Span* sp;

    for (int i=0;
        i < trace_data.resource_spans(0).scope_spans(0).spans_size();
        i++) {
        sp = &(trace_data.resource_spans(0).scope_spans(0).spans(i));
        std::string trace_id = hex_str(sp->trace_id(), sp->trace_id().length());
        time_t timestamp = sp->start_time_unix_nano();
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
                return Status(
                    ::google::cloud::StatusCode::kUnavailable,
                    "could not translate attribute");
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
    return to_return;
}

Status organize_data_into_nodes(
    const std::vector<std::string> &batches,
    const std::string attribute_to_index,
    std::map<time_t, Node> nodes,
    gcs::Client* client) {
    std::vector<std::future<StatusOr<std::vector<RawData>>>> data_futures;
    data_futures.reserve(batches.size());
    for (int64_t i=0; i < batches.size(); i++) {
        data_futures.push_back(std::async(std::launch::async,
            retrieve_single_batch_data,
            batches, i, attribute_to_index, client));
    }

    for (int64_t i=0; i < data_futures.size(); i++) {
        StatusOr<std::vector<RawData>> partial_data = data_futures[i].get();
        if (!partial_data.ok()) {
            return partial_data.status();
        }
        // for each piece of raw data, integrate into Nodes, but all in one large NodePieces
        for (int64_t j = 0; j < partial_data->size(); j++) {
            RawData* cur_data = &partial_data->at(i);
            time_t start_batch_time = cur_data->timestamp -
                (cur_data->timestamp % TIME_RANGE_PER_NODE);
            nodes[start_batch_time].data.push_back(IndexedData {
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
        std::to_string(start_time) +
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

    // Now, write the summary object.
    gcs::ObjectWriteStream stream = client->WriteObject(index_bucket,
        "summary-"+std::to_string(sum.start_time) +
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
         i < now - (now % summary_time); i++) {
        ret = create_summary_object(nodes, i,
            TIME_RANGE_PER_NODE*NUM_NODES_PER_SUMMARY, index_bucket, client);
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
    time_t granularity, last_updated;
    Status ret = get_last_updated_and_granularity(
        client, granularity, last_updated, index_bucket);
    if (!ret.ok()) {
        return ret;
    }

    // 2. Create nodes to be added.
    std::map<time_t, Node> nodes;
    time_t now;
    time(&now);
    now = now - (now % TIME_RANGE_PER_NODE);

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
    StatusOr<std::vector<RawData>> data = organize_data_into_nodes(
        batches, indexed_attribute, nodes, client);
    if (!data.ok()) {
        return data.status();
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
