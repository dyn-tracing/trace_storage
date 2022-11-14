
#include "range_index.h"

void Node::Serialize(std::ostream &os) {
    os.write((char *) &start_time, sizeof(time_t));
    os.write((char *) &end_time, sizeof(time_t));
}

Status Node::Deserialize(std::istream &is) {
    is.read(reinterpret_cast<char *>(&start_time), sizeof(time_t));
    is.read((char *) &end_time, sizeof(time_t));
    return Status();
}

void NodeSummary::Serialize(std::ostream &os){
    os.write((char *) &start_time, sizeof(time_t));
    os.write((char *) &end_time, sizeof(time_t));
}

Status NodeSummary::Deserialize(std::istream &is) {
    NodeSummary sum;
    is.read((char *) &sum.start_time, sizeof(time_t));
    is.read((char *) &sum.end_time, sizeof(time_t));
    return Status();
}

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
                    }
                );
            }
        }
    }
    return to_return;
}

StatusOr<std::vector<RawData>> organize_data_into_nodes(
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
            return partial_data;
        }
        // for each piece of raw data, integrate into Nodes
        for (int64_t j = 0; j < partial_data->size(); j++) {
            // TODO
        }
    }
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

    // Organize. Make nodes for all timestamps
    // From last_updated to now - other bits.
    // Because we modded now to TIME_RANGE_PER_NODE, we know that it
    // divides evenly

    // 4. Send that information to GCS.
}
