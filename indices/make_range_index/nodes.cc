#include "nodes.h"

void IndexedData::Serialize(std::ostream &os) {
    size_t size_batch_name, size_trace_id, size_data;
    size_batch_name = batch_name.size();
    size_trace_id = trace_id.size();
    size_data = data.size();
    os.write(reinterpret_cast<char *>(&size_batch_name), sizeof(size_t));
    os.write(reinterpret_cast<char *>(&size_trace_id), sizeof(size_t));
    os.write(reinterpret_cast<char *>(&size_data), sizeof(size_t));
    os.write(reinterpret_cast<const char *>(batch_name.c_str()),
             size_batch_name);
    os.write(reinterpret_cast<const char *>(trace_id.c_str()), size_trace_id);
    os.write(reinterpret_cast<const char *>(data.c_str()), size_data);
}

std::string read_str(std::istream &is, size_t size_to_read) {
    char * buffer = new char[size_to_read+1];
    is.read(buffer, size_to_read);
    buffer[size_to_read] = '\0';
    std::string str(buffer);
    delete [] buffer;
    return str;
}

Status IndexedData::Deserialize(std::istream &is) {
    size_t size_batch_name, size_trace_id, size_data;
    is.read(reinterpret_cast<char *>(&size_batch_name), sizeof(size_t));
    is.read(reinterpret_cast<char *>(&size_trace_id), sizeof(size_t));
    is.read(reinterpret_cast<char *>(&size_data), sizeof(size_t));
    batch_name = read_str(is, size_batch_name);
    trace_id = read_str(is, size_trace_id);
    data = read_str(is, size_data);
    return Status();
}

void Node::Serialize(std::ostream &os) {
    os.write(reinterpret_cast<char *>(&start_time), sizeof(time_t));
    os.write(reinterpret_cast<char *>(&end_time), sizeof(time_t));
    size_t num_data = data.size();
    os.write(reinterpret_cast<char *>(&num_data), sizeof(size_t));
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

int64_t count_bytes_of_data(IndexedData d) {
    return d.batch_name.size() + d.trace_id.size() + d.data.size();
}

std::vector<Node> Node::Split() const {
    std::vector<Node> to_return;
    Node building = {
        .start_time = start_time,
        .end_time = end_time
    };
    int64_t byte_count = sizeof(time_t)*3;
    for (int64_t i=0; i < data.size(); i++) {
        building.data.push_back(data[i]);
        byte_count += count_bytes_of_data(data[i]);
        if (byte_count > NUM_BYTES_IN_GIGABYTE) {
            to_return.push_back(building);
            byte_count = sizeof(time_t)*3;
            building = Node {.start_time = start_time, .end_time = end_time};
        }
    }
    to_return.push_back(building);
    return to_return;
}

void NodeSummary::Serialize(std::ostream &os) {
    os.write(reinterpret_cast<char *>(&start_time), sizeof(time_t));
    os.write(reinterpret_cast<char *>(&end_time), sizeof(time_t));
    size_t num_node_objects = node_objects.size();
    os.write(reinterpret_cast<char *>(&num_node_objects), sizeof(size_t));
    for (int64_t i=0; i < num_node_objects; i++) {
        os.write(reinterpret_cast<char *>(&std::get<0>(node_objects[i])),
                 sizeof(time_t));
        size_t len_str = std::get<1>(node_objects[i]).size();
        os.write(reinterpret_cast<char *>(&len_str), sizeof(size_t));
        os.write(reinterpret_cast<const char *>
                (std::get<1>(node_objects[i]).c_str()),
                len_str);
    }
}

Status NodeSummary::Deserialize(std::istream &is) {
    is.read(reinterpret_cast<char *>(&start_time), sizeof(time_t));
    is.read(reinterpret_cast<char *>(&end_time), sizeof(time_t));
    size_t num_node_objects;
    is.read(reinterpret_cast<char *>(&num_node_objects), sizeof(size_t));
    for (int64_t i=0; i< num_node_objects; i++) {
        time_t t;
        is.read(reinterpret_cast<char *>(&t), sizeof(time_t));
        size_t str_size;
        is.read(reinterpret_cast<char *>(&str_size), sizeof(size_t));
        std::string str = read_str(is, str_size);
        node_objects.push_back(std::make_pair(t, str));
    }
    return Status();
}

