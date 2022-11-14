
#include <vector>
#include "google/cloud/storage/client.h"

#include "common.h"

const int64_t TIME_RANGE_PER_NODE = 10;
const int64_t  NUM_NODES_PER_SUMMARY = 20;

struct IndexedData {
    std::string batch_name;
    std::string trace_id;
    std::string data;

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);
};

struct Node {
    time_t start_time;
    time_t end_time;
    std::vector<IndexedData> data;

    void Serialize(std::ostream &os);
    Status Deserialize(std::istream &is);

    // Split into roughly 1 GB Nodes.
    std::vector<Node> Split();
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
