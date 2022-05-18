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

/**
 * TODO: No need to keep it a struct now as it contains only one variable ... but it has already been used many times.
 */
struct index_batch {
	std::vector<std::pair<std::string, std::unordered_map<std::string, std::vector<std::string>>>>
	trace_ids_with_timestamps;  // vector of pairs, a pair is (timestamp, a map)
								// the map maps the trace ids (values) to the attribute values (keys)
								// e.g. one map => {"error 404": {trace1, trace2}, "error 500": {trace3, trace1}}
};

namespace gcs = ::google::cloud::storage;
using ::google::cloud::StatusOr;

void update_bucket_label(std::string bucket_name, std::string label_key, std::string label_val, gcs::Client* client);
std::vector<std::string> get_all_attr_values(index_batch& current_index_batch);
int get_total_of_trace_ids(std::unordered_map<std::string, std::vector<std::string>> attr_to_trace_ids);
void write_object(std::string bucket_name, std::string object_name,
	std::string& object_to_write, gcs::Client* client);
int update_index(gcs::Client* client, time_t last_updated, 
	std::string indexed_attribute
);
std::string get_autoscaling_hash_from_start_time(std::string start_time);
std::string serialize_trace_ids(std::vector<std::string>& trace_ids);
std::unordered_map<std::string, std::vector<std::string>> calculate_attr_to_trace_ids_map_for_microservice(
	std::string span_bucket_name, std::string object_name, std::string indexed_attribute, gcs::Client* client
);
std::vector<std::string> get_spans_buckets_names(gcs::Client* client);
batch_timestamp extract_batch_timestamps(std::string batch_name);
std::vector<std::string> get_all_object_names(std::string bucket_name, gcs::Client* client);
std::vector<std::string> sort_object_names_on_start_time(std::vector<std::string> object_names);
std::unordered_map<std::string, std::vector<std::string>> get_attr_to_trace_ids_map(
	std::string object_name, std::string indexed_attribute,
	std::vector<std::string>& span_buckets_names, gcs::Client* client
);
void take_per_field_union(std::unordered_map<std::string, std::vector<std::string>>& attr_to_trace_ids_map,
	std::unordered_map<std::string, std::vector<std::string>>& local_attr_to_trace_ids_map
);
std::vector<std::string> get_attr_vals_which_have_enough_data_to_export(index_batch& current_index_batch);
void remove_exported_data_from_index_batch(index_batch& current_index_batch, std::string attr_to_remove);
void export_batch_to_storage(index_batch& current_index_batch, std::string indexed_attribute,
	std::vector<std::string> attrs_to_export, gcs::Client* client
);
std::vector<std::string> split_by_char(std::string input, std::string splitter);
bool compare_object_names_by_start_time(std::string object_name1, std::string object_name2);
std::string read_object(std::string bucket, std::string object, gcs::Client* client);
std::vector<std::string> split_by_string(std::string input, std::string splitter);
std::string strip_from_the_end(std::string object, char stripper);
std::string hex_str(std::string data, int len);
void create_index_bucket_if_not_present(std::string indexed_attribute, gcs::Client* client);
void print_index_batch(index_batch& current_index_batch);
int dummy_tests();

#endif  // STATUS_CODE_INDEX_H_