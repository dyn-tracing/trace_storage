
#include <vector>
#include "google/cloud/storage/client.h"

#include "common.h"

struct node {
    time_t start_time;
    time_t end_time;
    int64_t start_value;
    int64_t end_value;
    std::vector<int64_t> values;
}

struct node_summary {
    time_t start_time;
    time_t end_time;
    std::vector<int64_t> node_objects;
}

inline void Serialize(std::ostream &os, node &quant) const;
inline void Deserialize(std::istream &is, node &quant) const;
inline void Serialize(std::ostream &os, node_summary &sum) const;
inline void Deserialize(std::istream &is, node_summary &sum) const;

Status update(std::string indexed_attribute, gcs::Client* client);
