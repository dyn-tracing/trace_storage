#include "nodes.h"

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

void IndexedData::Serialize(std::ostream &os) {
}

Status IndexedData::Deserialize(std::istream &is) {

}
