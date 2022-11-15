#include "common.h"

struct IndexedData {
    std::string batch_name;
    std::string trace_id;
    std::string data;

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);
    bool operator==(const IndexedData &other) const {
        return batch_name == other.batch_name &&
               trace_id == other.trace_id &&
               data == other.data;
    }
};

struct Node {
    time_t start_time;
    time_t end_time;
    std::vector<IndexedData> data;

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);
    bool operator==(const Node &other) const {
        return start_time == other.start_time &&
               end_time == other.end_time &&
               data == other.data;
    }

    // Split into roughly 1 GB Nodes.
    std::vector<Node> Split() const;
};

struct NodeSummary {
    time_t start_time;
    time_t end_time;
    // Map from start time to the first value in that batch.
    std::vector<std::pair<time_t, std::string>> node_objects;
    bool operator==(const NodeSummary &other) const {
        return start_time == other.start_time &&
               end_time == other.end_time &&
               node_objects == other.node_objects;
    }

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);
};
