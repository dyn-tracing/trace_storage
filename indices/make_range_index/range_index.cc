
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
    // TODO
}
NodeSummary DeserializeNodeSummary(std::istream &is, NodeSummary &sum){
    // TODO
}

Status update(std::string indexed_attribute, gcs::Client* client) {
    // TODO
}
