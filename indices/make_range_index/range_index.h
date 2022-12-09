#ifndef INDICES_MAKE_RANGE_INDEX_RANGE_INDEX_H_
#define INDICES_MAKE_RANGE_INDEX_RANGE_INDEX_H_
#include <string>
#include <vector>
#include "google/cloud/storage/client.h"

#include "common.h"
#include "nodes.h"


const int64_t TIME_RANGE_PER_NODE = 100;
const int64_t  NUM_NODES_PER_SUMMARY = 5;

struct RawData {
    int64_t batch_index;
    time_t timestamp;
    std::string trace_id;
    std::string data;
};

Status update(std::string indexed_attribute, gcs::Client* client);

#endif  // INDICES_MAKE_RANGE_INDEX_RANGE_INDEX_H_
