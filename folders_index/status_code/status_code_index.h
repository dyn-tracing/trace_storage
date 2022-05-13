// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#ifndef STATUS_CODE_INDEX_H_
#define STATUS_CODE_INDEX_H_

#include <algorithm>
#include <future>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "trace_attributes.h"
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include <boost/algorithm/string.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

const char BUCKET_TYPE_LABEL_KEY[] = "bucket_type";
const char BUCKET_TYPE_LABEL_VALUE_FOR_SPAN_BUCKETS[] = "microservice";

const char TRACE_STRUCT_BUCKET[] = "dyntraces-snicket3";
const char TRACE_HASHES_BUCKET[] = "tracehashes-snicket3";
const char SERVICES_BUCKETS_SUFFIX[] = "-snicket3";
const char INDEX_STATUS_CODE_BUCKET[] = "index-status-code-snicket3";
const char INDEX_SPAN_KIND_BUCKET[] = "index-span-kind";
const char PROJECT_ID[] = "dynamic-tracing";
const char BUCKETS_LOCATION[] = "us-central1";

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
	std::vector<std::pair<std::string, std::unordered_map<std::string, std::vector<std::string>>>>
	trace_ids_with_timestamps;  // vector of pairs, a pair is (timestamp, a map)
								// the map maps the trace ids to the attribute values 
								// e.g. one map => {"trace xyz": {"error 404", "error 202"}, "trace abc": {"error 500"}}
	index_batch() {
		total_trace_ids = 0;
	}
};

namespace gcs = ::google::cloud::storage;
using ::google::cloud::StatusOr;

void write_object(std::string bucket_name, std::string object_name,
	std::string& object_to_write, gcs::Client* client);
int update_index(gcs::Client* client, time_t last_updated, 
	std::string indexed_attribute, std::string attribute_value
);
std::string get_autoscaling_hash_from_start_time(std::string start_time);
std::string serialize_trace_ids(std::vector<std::string>& trace_ids);
std::unordered_map<std::string, std::vector<std::string>> calculate_trace_id_to_attribute_map(
	std::string span_bucket_name, std::string object_name, std::string indexed_attribute,
	std::string attribute_value, gcs::Client* client
);
std::vector<std::string> get_spans_buckets_names(gcs::Client* client);
batch_timestamp extract_batch_timestamps(std::string batch_name);
std::vector<std::string> get_all_object_names(std::string bucket_name, gcs::Client* client);
std::vector<std::string> sort_object_names_on_start_time(std::vector<std::string> object_names);
std::unordered_map<std::string, std::vector<std::string>> get_trace_ids_with_attribute(
	std::string object_name, std::string indexed_attribute, std::string attribute_value,
	std::vector<std::string>& span_buckets_names, gcs::Client* client
);
void take_per_field_union(std::unordered_map<std::string, std::vector<std::string>>& trace_id_to_attribute_membership,
	std::unordered_map<std::string, std::vector<std::string>>& local_trace_id_to_attribute_membership
);
bool is_batch_big_enough(index_batch& current_index_batch);
void export_batch_to_storage(index_batch& current_index_batch,
	std::string indexed_attribute, std::string attribute_value, gcs::Client* client
);
std::vector<std::string> split_by_char(std::string input, std::string splitter);
bool compare_object_names_by_start_time(std::string object_name1, std::string object_name2);
std::string read_object(std::string bucket, std::string object, gcs::Client* client);
std::vector<std::string> split_by_string(std::string input, std::string splitter);
std::string strip_from_the_end(std::string object, char stripper);
std::string hex_str(std::string data, int len);
void create_index_bucket_if_not_present(std::string indexed_attribute, gcs::Client* client);

int dummy_tests();

#endif  // STATUS_CODE_INDEX_H_