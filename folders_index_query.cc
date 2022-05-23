#include "folders_index_query.h"

int main() {
	auto client = gcs::Client();
	auto res = get_obj_name_to_trace_ids_map_from_folders_index(ATTR_HTTP_STATUS_CODE, "500", &client);
	print_folders_index_query_res(res);
	return 0;
}

std::unordered_map<std::string, std::vector<std::string>> get_obj_name_to_trace_ids_map_from_folders_index(
	std::string attr_key, std::string attr_val, gcs::Client* client
) {
	std::unordered_map<std::string, std::vector<std::string>> response;
	std::string bucket_name = get_bucket_name_for_attr(attr_key);
	std::string folder = get_folder_name_from_attr_value(attr_val);

	for (auto&& prefix : client->ListObjectsAndPrefixes(bucket_name, gcs::Delimiter("/"))) {
		if (!prefix) {
			std::cerr << "Error in getting prefixes" << std::endl;
			exit(1);
		}

		auto result = *std::move(prefix);
		if (false == absl::holds_alternative<std::string>(result)) {
			std::cerr << "Error in getting prefixes" << std::endl;
			exit(1);
		}
		std::string prefix_ = absl::get<std::string>(result);

		std::cout << prefix_ << std::endl;
	}

	exit(1);
	return response;
}

std::unordered_map<std::string, std::vector<std::string>> process_findex_prefix_and() {
	
}
void print_folders_index_query_res(std::unordered_map<std::string, std::vector<std::string>> res) {

}
