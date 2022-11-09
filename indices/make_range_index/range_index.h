
#include <vector>
#include "google/cloud/storage/client.h"

#include "common.h"

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

inline void Serialize(std::ostream &os, Node &quant)  ;
inline void Deserialize(std::istream &is, Node &quant)  ;
inline void Serialize(std::ostream &os, NodeSummary &sum)  ;
inline void Deserialize(std::istream &is, NodeSummary &sum)  ;

Status update(std::string indexed_attribute, gcs::Client* client);
