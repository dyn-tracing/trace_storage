
#include "range_index.h"

void Serialize(std::ostream &os, Node &node) {
    os.write((char *) &node.start_time, sizeof(time_t));
    os.write((char *) &node.end_time, sizeof(time_t));
    os.write((char *) &node.start_value, sizeof(int64_t));
    os.write((char *) &node.end_value, sizeof(int64_t));
    size_t size = node.values.size();
    os.write((char *) &size, sizeof(size_t));
    for (unsigned int i=0; i < node.values.size(); i++) {
        os.write((char *) &node.values[i], sizeof(int64_t));
    }
}

Node DeserializeNode(std::istream &is){
    Node node;
    is.read((char *) &node.start_time, sizeof(time_t));
    is.read((char *) &node.end_time, sizeof(time_t));
    is.read((char *) &node.start_value, sizeof(int64_t));
    is.read((char *) &node.end_value, sizeof(int64_t));
    size_t values_size;
    is.read((char *) &values_size, sizeof(size_t));
    node.values.reserve(values_size);
    for (unsigned int i=0; i<values_size; i++) {
        int64_t val;
        is.read((char *) &val, sizeof(int64_t));
        node.values.push_back(val);
    }
    return node;
}

void Serialize(std::ostream &os, NodeSummary &sum){
    os.write((char *) &sum.start_time, sizeof(time_t));
    os.write((char *) &sum.end_time, sizeof(time_t));
    size_t size = sum.node_objects.size();
    os.write((char *) &size, sizeof(size_t));
    for (unsigned int i=0; i<size; i++) {
        os.write((char *) &sum.node_objects[i], sizeof(int64_t));
    }
}

NodeSummary DeserializeNodeSummary(std::istream &is) {
    NodeSummary sum;
    is.read((char *) &sum.start_time, sizeof(time_t));
    is.read((char *) &sum.end_time, sizeof(time_t));
    size_t size;
    is.read((char *) &size, sizeof(size_t));
    sum.node_objects.reserve(size);
    for (unsigned int i=0; i<size; i++) {
        is.read((char *) &sum.node_objects[i], sizeof(int64_t));
    }
    return sum;
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

    for (int i=0; i < trace_data.resource_spans(0).scope_spans(0).spans_size(); i++) {
        sp = &(trace_data.resource_spans(0).scope_spans(0).spans(i));           
                                                                                
        std::string trace_id = hex_str(sp->trace_id(), sp->trace_id().length());
        time_t timestamp = sp->start_time_unix_nano();
                                                                                
        const opentelemetry::proto::common::v1::KeyValue* attribute;            
        for (int j=0; j < sp->attributes_size(); j++) {                         
            attribute =  &(sp->attributes(j));                                  
            const opentelemetry::proto::common::v1::AnyValue* val = &(attribute->value());
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
                    ::google::cloud::StatusCode::kUnavailable, "could not translate attribute");
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

StatusOr<std::vector<RawData>> retrieve_data(
    const std::vector<std::string> &batches,
    const std::string attribute_to_index,
    gcs::Client* client) {
    std::vector<std::future<StatusOr<std::vector<RawData>>>> data_futures;
    data_futures.reserve(batches.size());
    for (int64_t i=0; i<batches.size(); i++) {
        data_futures.push_back(std::async(std::launch::async,
            retrieve_single_batch_data, batches, i, attribute_to_index, client));
    }
    std::vector<RawData> all_data;
    for (int64_t i=0; i<data_futures.size(); i++) {
        StatusOr<std::vector<RawData>> partial_data = data_futures[i].get();
        if (!partial_data.ok()) {
            return partial_data;
        }
        all_data.insert(all_data.end(),
                        partial_data->begin(),
                        partial_data->end());
    }
    return all_data;
}

Status update(std::string indexed_attribute, gcs::Client* client) {
    std::string index_bucket = indexed_attribute + "-range-index";
    replace_all(index_bucket, ".", "-");

    // 1. Find granularity and last updated
    time_t granularity, last_updated;
    Status ret = get_last_updated_and_granularity(client, granularity, last_updated, index_bucket);
    if (!ret.ok()) {
        return ret;
    }

    // 2. Find all objects that have happened since last updated
    time_t now;
    time(&now);
    std::vector<std::string> batches = get_batches_between_timestamps(client, last_updated, now);

    // 3. Organize their data into summary and node objects
    //    To do so, we need to know (1) batch name, (2) timestamp, (3) trace ID, (4) data
    StatusOr<std::vector<RawData>> data = retrieve_data(batches, indexed_attribute, client);

    // 4. Send that information to GCS.
}
