
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
    // 4. Send that information to GCS.
}
