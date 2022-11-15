#include "nodes.h"

void IndexedData::Serialize(std::ostream &os) {
}

Status IndexedData::Deserialize(std::istream &is) {
}

void Node::Serialize(std::ostream &os) {
    os.write((char *) &start_time, sizeof(time_t));
    os.write((char *) &end_time, sizeof(time_t));
    const size_t num_data = data.size();
    os.write((char *) &num_data, sizeof(size_t));
    for (int64_t i=0; i < num_data; i++) {
        data[i].Serialize(os);
    }
}

Status Node::Deserialize(std::istream &is) {
    is.read(reinterpret_cast<char *>(&start_time), sizeof(time_t));
    is.read(reinterpret_cast<char *>(&end_time), sizeof(time_t));
    size_t num_data;
    is.read(reinterpret_cast<char *>(&num_data), sizeof(size_t));
    for (int64_t i=0; i < num_data; i++) {
        IndexedData piece_of_data;
        piece_of_data.Deserialize(is);
        data.push_back(piece_of_data);
    }
    return Status();
}

std::vector<Node> Node::Split() const {

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

