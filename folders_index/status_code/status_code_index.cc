// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#include "status_code_index.h"

int main(int argc, char* argv[]) {
	dummy_tests();

	auto client = gcs::Client();
	time_t last_updated = 0;
	return update_index(&client, last_updated, "202");
}

int update_index(gcs::Client* client, time_t last_updated, std::string index_status_code) {
	/**
	 * TODO: (i) Following is a bad thing to do. what if all object names do not fit in the memory. 
	 * (ii) do the error handling for the case when bucket is not present. 
	 */
	vector<std::string> trace_struct_object_names = get_all_object_names(TRACE_STRUCT_BUCKET);
	trace_struct_object_names = sort_object_names_on_start_time(trace_struct_object_names);
	index_batch current_index_batch = index_batch();

	for (auto object_name : trace_struct_object_names) {
		std::vector<std::string> trace_ids_with_index_status_code = get_trace_ids_with_index_status_code(object_name, index_status_code);
		
		current_index_batch.total_trace_ids += trace_ids_with_index_status_code.size();
		current_index_batch.trace_ids_with_timestamps.push_back(std::make_pair(
			extract_batch_timestamps(object_name), trace_ids_with_index_status_code
		));

		if (true == is_batch_big_enough(current_index_batch)) {
			auto status = export_batch_to_storage(current_index_batch, index_status_code);
			if (status != 0) {
				std::cout << "Could not export an index batch to Cloud Storage!" << std::endl;
				exit(1);
			}

			current_index_batch = index_batch();
		}
	}
	return 0;
}

vector<std::string> get_all_object_names(std::string bucket_name) {
	// TODO
}

vector<std::string> sort_object_names_on_start_time(vector<std::string> object_names) {
	// TODO
}

std::vector<std::string> get_trace_ids_with_index_status_code(std::string object_name, std::string status_code) {
	// TODO
}

batch_timestamp extract_batch_timestamps(std::string batch_name) {
	std::vector<std::string> result;
	boost::split(result, batch_name, boost::is_any_of("-"));
	if (result.size() != 3) {
		std::cerr << "Error in extract_batch_timestamps with batch name: " << batch_name << std::endl;
	}

	batch_timestamp timestamp = {result[1], result[2]};
	return timestamp;
}

bool is_batch_big_enough(index_batch& current_index_batch) {
	// TODO
}

int export_batch_to_storage(index_batch& current_index_batch, std::string index_status_code) {
	// TODO
}

int dummy_tests() {

	// exit(1);
	return 0;
}