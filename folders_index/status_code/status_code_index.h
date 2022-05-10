// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#ifndef STATUS_CODE_INDEX_H_
#define STATUS_CODE_INDEX_H_

#include <iostream>
#include <unordered_map>
#include <utility>
#include <map>
#include <string>
#include <vector>
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include <boost/algorithm/string.hpp>

const char TRACE_STRUCT_BUCKET[] = "dyntraces-snicket4";
const char TRACE_HASHES_BUCKET[] = "tracehashes-snicket4";
const char SERVICES_BUCKETS_SUFFIX[] = "-snicket4";
const char INDEX_STATUS_CODE_BUCKET[] = "index-status-code-snicket4";

struct batch_timestamp {
    std::string start_time;
    std::string end_time;

    batch_timestamp() {
        start_time = "";
        end_time = "";
    }

    batch_timestamp(std::string start_time_, std::string end_time_) {
        start_time = start_time_;
        end_time = end_time_;
    }
};

struct index_batch {
    int total_trace_ids;
    std::vector<std::pair<batch_timestamp, std::vector<std::string>>> trace_ids_with_timestamps;
    index_batch() {
        total_trace_ids = 0;
    }
};

namespace gcs = ::google::cloud::storage;
using ::google::cloud::StatusOr;

int update_index(gcs::Client* client, time_t last_updated, std::string index_status_code);
batch_timestamp extract_batch_timestamps(std::string batch_name);
std::vector<std::string> get_all_object_names(std::string bucket_name);
std::vector<std::string> sort_object_names_on_start_time(std::vector<std::string> object_names);
std::vector<std::string> get_trace_ids_with_index_status_code(std::string object_name, std::string status_code);
bool is_batch_big_enough(index_batch& current_index_batch);
int export_batch_to_storage(index_batch& current_index_batch, std::string index_status_code);
int dummy_tests();

#endif // STATUS_CODE_INDEX_H_