
#include <vector>
#include "google/cloud/storage/client.h"

#include "common.h"

const int64_t TIME_RANGE_PER_NODE = 10;
const int64_t  NUM_NODES_PER_SUMMARY = 20;

struct IndexedData {
    std::string batch_name;
    std::string trace_id;
    int64_t data;

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);
};

struct NodePieces {
    int64_t start_value;
    int64_t end_value;
    std::vector<IndexedData> values;

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);
};

struct Node {
    time_t start_time;
    time_t end_time;
    // Each piece should be roughly 1 GB.
    std::vector<NodePieces> node_pieces;

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);
};

struct NodeID {
    time_t start_time;
    time_t end_time;
    int64_t num_node_pieces;

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);
};

struct NodeSummary {
    time_t start_time;
    time_t end_time;
    std::vector<int64_t> node_objects;

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);
};

struct RawData {
    int64_t batch_index;
    time_t timestamp;
    std::string trace_id;
    std::string data;
};

Status update(std::string indexed_attribute, gcs::Client* client);
