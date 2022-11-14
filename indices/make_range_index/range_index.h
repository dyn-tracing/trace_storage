
#include <vector>
#include "google/cloud/storage/client.h"

#include "common.h"

const int64_t TIME_RANGE_PER_NODE = 10;
const int64_t  NUM_NODES_PER_SUMMARY = 20;

struct Node {
    time_t start_time;
    time_t end_time;
    int64_t start_value;
    int64_t end_value;
    std::vector<int64_t> values;
};

struct NodeSummary {
    time_t start_time;
    time_t end_time;
    std::vector<int64_t> node_objects;
};

struct RawData {
    int64_t batch_index;
    time_t timestamp;
    std::string trace_id;
    std::string data;
};

void Serialize(std::ostream &os, Node &node);
Node DeserializeNode(std::istream &is);
void Serialize(std::ostream &os, NodeSummary &sum);
NodeSummary DeserializeNodeSummary(std::istream &is);

Status update(std::string indexed_attribute, gcs::Client* client);
