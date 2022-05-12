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

const char BUCKET_TYPE_LABEL_KEY[] = "bucket_type";
const char BUCKET_TYPE_LABEL_VALUE_FOR_SPAN_BUCKETS[] = "microservice";

const char TRACE_STRUCT_BUCKET[] = "dyntraces-snicket3";
const char TRACE_HASHES_BUCKET[] = "tracehashes-snicket3";
const char SERVICES_BUCKETS_SUFFIX[] = "-snicket3";
const char INDEX_STATUS_CODE_BUCKET[] = "index-status-code-snicket3";
const char PROJECT_ID[] = "dynamic-tracing";

enum trace_attribute {
    HTTP_STATUS_CODE,
    IS_A_GOOD_TRACE  // just an example
};

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

int update_index(gcs::Client* client, time_t last_updated, 
	trace_attribute indexed_attribute, std::string attribute_value
);
std::unordered_map<std::string, bool> calculate_trace_id_to_attribute_map(std::string span_bucket_name,
	std::string object_name, trace_attribute indexed_attribute, std::string attribute_value, gcs::Client* client
);
std::vector<std::string> get_spans_buckets_names(gcs::Client* client);
batch_timestamp extract_batch_timestamps(std::string batch_name);
std::vector<std::string> get_all_object_names(std::string bucket_name, gcs::Client* client);
std::vector<std::string> sort_object_names_on_start_time(std::vector<std::string> object_names);
std::vector<std::string> get_trace_ids_with_attribute(
	std::string object_name, trace_attribute indexed_attribute, std::string attribute_value,
	std::vector<std::string>& span_buckets_names, gcs::Client* client
);
void take_per_field_OR(std::unordered_map<std::string, bool>& trace_id_to_attribute_membership,
	std::unordered_map<std::string, bool>& local_trace_id_to_attribute_membership);
bool is_batch_big_enough(index_batch& current_index_batch);
int export_batch_to_storage(index_batch& current_index_batch, std::string index_status_code);
std::vector<std::string> split_by_char(std::string input, std::string splitter);
bool compare_object_names_by_start_time(std::string object_name1, std::string object_name2);
std::string read_object(std::string bucket, std::string object, gcs::Client* client);
std::vector<std::string> split_by_string(std::string input, std::string splitter);
std::string strip_from_the_end(std::string object, char stripper);
std::string hex_str(std::string data, int len);

int dummy_tests();

#endif // STATUS_CODE_INDEX_H_