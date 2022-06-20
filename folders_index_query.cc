#include "folders_index_query.h"

std::map<std::string, std::vector<std::string>> get_obj_name_to_trace_ids_map_from_folders_index(
	std::string attr_key, std::string attr_val, int start_time, int end_time, gcs::Client* client
) {
	std::vector<std::future<std::unordered_map<std::string, std::vector<std::string>>>> response_futures;
	std::string bucket_name = get_bucket_name_for_attr(attr_key);
	std::string folder = get_folder_name_from_attr_value(attr_val) + "/";

	for (auto&& object_metadata : client->ListObjects(bucket_name, gcs::Prefix(folder))) {
		if (!object_metadata) {
			std::cerr << object_metadata.status().message() << std::endl;
			exit(1);
		}

        if (false == is_object_within_timespan(
                extract_batch_timestamps(object_metadata->name()), start_time, end_time)) {
            continue;
        }

		response_futures.push_back(std::async(
			std::launch::async,
			process_findex_object_and_retrieve_obj_name_to_trace_ids_map,
			object_metadata->name(), bucket_name, start_time, end_time, client));
	}

	std::map<std::string, std::vector<std::string>> response;
	for_each(response_futures.begin(), response_futures.end(),
		[&response](std::future<std::unordered_map<std::string, std::vector<std::string>>>& fut) {
			std::unordered_map<std::string, std::vector<std::string>> obj_name_to_trace_ids_map = fut.get();
			for (auto& ele : obj_name_to_trace_ids_map) {
				response[ele.first] = ele.second;
			}
	});

	return response;
}

std::unordered_map<std::string, std::vector<std::string>>
process_findex_object_and_retrieve_obj_name_to_trace_ids_map(
	std::string findex_obj_name, std::string findex_bucket_name, int start_time, int end_time, gcs::Client* client
) {
	std::unordered_map<std::string, std::vector<std::string>> response;

	auto object_content = read_object(findex_bucket_name, findex_obj_name, client);
	auto sections = split_by_string(object_content, "Timestamp: ");

	for (auto& curr_section : sections) {
		auto lines = split_by_string(curr_section, newline);
		auto obj_name = lines[0];
        if (false == is_object_within_timespan(extract_batch_timestamps(obj_name), start_time, end_time)) {
            continue;
        }
		std::vector<std::string> trace_ids;
		for (int i = 1; i < lines.size(); i++) {
			if (lines[i].length() > 1) {
				trace_ids.push_back(lines[i]);
			}
		}
		response[obj_name] = trace_ids;
	}
	return response;
}

void print_folders_index_query_res(std::unordered_map<std::string, std::vector<std::string>> res) {
	for (auto obj_name_and_trace_ids : res) {
		std::cout << obj_name_and_trace_ids.first << std::endl;
		for (auto curr_trace_id : obj_name_and_trace_ids.second) {
			std::cout << curr_trace_id << ", " << std::flush;
		}
		std::cout << std::endl;
	}
}
