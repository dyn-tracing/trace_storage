// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#ifndef STATUS_CODE_INDEX_H_  // NOLINT
#define STATUS_CODE_INDEX_H_  // NOLINT

#include <algorithm>
#include <future>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "common.h"
#include "trace_attributes.h"
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include <boost/algorithm/string.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

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

void update_index_batched(gcs::Client* client, time_t last_updated, std::string indexed_attribute,
	std::vector<std::string> span_buckets_names, std::vector<std::string>& trace_struct_object_names,
	int batch_start_ind, int batch_size
);
void update_last_updated_label_if_needed(
	std::string bucket_name, std::string new_last_updated, gcs::Client* client
);
bool is_batch_older_than_last_updated(std::string batch_name, time_t last_updated);
time_t get_last_updated_for_bucket(std::string bucket_name, gcs::Client* client);
std::string read_bucket_label(std::string bucket_name, std::string label_key, gcs::Client* client);
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
batch_timestamp extract_batch_timestamps_struct(std::string batch_name);
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
void create_index_bucket_if_not_present(std::string indexed_attribute, gcs::Client* client);
void print_index_batch(index_batch& current_index_batch);
int dummy_tests();

#endif  // STATUS_CODE_INDEX_H_   // NOLINT
