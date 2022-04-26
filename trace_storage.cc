#include "trace_storage.h"

int main(int argc, char* argv[]) {
	// dummy_tests();
	// exit(1);

	if (argc != 2) {
		std::cerr << "Missing bucket name.\n";
		std::cerr << "Usage: quickstart <bucket-name>\n";
		return 1;
	}
	std::string const bucket_name = argv[1];

	auto client = gcs::Client();

	int total = get_traces_by_structure("checkoutservice", "cartservice", 1650574264, 1650574264, &client);
	std::cout << "Total results: " << total << std::endl;
	return 0;
}

/**
 * @brief Get the traces that conform to the given structure
 * 
 * @param parent_service 
 * @param child_service 
 * @param start_time 
 * @param end_time 
 * @param client 
 * @return int 
 */
int get_traces_by_structure(std::string parent_service, std::string child_service, int start_time, int end_time, gcs::Client* client) {
	std::vector<std::string> response;

	for (auto&& object_metadata : client->ListObjects(trace_hashes_bucket)) {
		if (!object_metadata) {
			std::cerr << object_metadata.status().message() << std::endl;
		}

		std::string object_name = object_metadata->name();
		std::string batch_name = extract_batch_name(object_name);

		if (false == is_object_within_timespan(batch_name, start_time, end_time)) {
			continue;
		}

		std::vector<std::string> trace_ids = get_trace_ids_from_trace_hashes_object(object_name, client);
		std::cout << object_name << std::endl;
		if (trace_ids.size() < 1) {
			continue;
		}

		if (false == does_trace_structure_conform_to_graph_query(batch_name, trace_ids[0], parent_service, child_service, client)) {
			continue;
		}
		
		response.insert(response.end(), trace_ids.begin(), trace_ids.end());

		break;
	}

	return response.size();
}

bool does_trace_structure_conform_to_graph_query(std::string traces_object, std::string trace_id, std::string parent_service, std::string child_service, gcs::Client* client ) {
	
	std::cout << traces_object << std::endl;
	return true;
}

std::vector<std::string> split_by_line(std::string input) {
	std::vector<std::string> result;
	boost::split(result, input, boost::is_any_of("\n"));
	if (result[result.size()-1].length() < 1) {
		result.pop_back();
	}

	return result;
}

/**
 * @brief Right now this function only calculates whether the batch timestamps have some overlap with the 
 * timespan provided in the query. TODO: Don't find just the overlap, calculate whether the object
 * is really between the provided timespan by reading the timestamps of the object rather than just using the 
 * timespans of the batch. 
 * 
 * @param object_name 
 * @param start_time 
 * @param end_time 
 * @return bool 
 */
bool is_object_within_timespan(std::string object_name, int start_time, int end_time) {

	// Making pairs just for the sake of readability
	std::pair<int, int> batch_time = extract_batch_timestamps(object_name);
	std::pair<int, int> query_timespan = std::make_pair(start_time, end_time); 
	
	// query timespan between object timespan
	if (batch_time.first <= query_timespan.first && batch_time.second >= query_timespan.second) {
		return true;
	}

	// query timespan contains object timespan
	if (batch_time.first >= query_timespan.first && batch_time.second <= query_timespan.second) {
		return true;
	}

	// batch timespan overlaps but starts before query timespan
	if (batch_time.first <= query_timespan.first && batch_time.second <= query_timespan.second 
	&& batch_time.second >= query_timespan.first) {
		return true;
	}

	// vice versa
	if (batch_time.first >= query_timespan.first && batch_time.second >= query_timespan.second 
	&& batch_time.first <= query_timespan.second) {
		return true;
	}

	return false;
}

std::string read_object(std::string bucket, std::string object, gcs::Client* client) {
	auto reader = client->ReadObject(bucket, object);
	if (!reader) {
		std::cerr << "Error reading object: " << reader.status() << "\n";
		exit(1);
	}

	std::string object_content{std::istreambuf_iterator<char>{reader}, {}};
	return object_content;
}

std::vector<std::string> get_trace_ids_from_trace_hashes_object(std::string object_name, gcs::Client* client) {
	std::string object_content = read_object(trace_hashes_bucket, object_name, client);
	std::vector<std::string> trace_ids = split_by_line(object_content);

	return trace_ids;
}

int get_trace(std::string traceID, int start_time, int end_time, gcs::Client* client) {
    bool trace_found = false;
    for (int i=0; i<10; i++) {
        if (trace_found) {
            break;
        }
        for (int j=0; j<10; j++) {
          if (trace_found) {
            break;
          }
          std::string obj_name = std::to_string(i) + std::to_string(j) + "-";
          obj_name += std::to_string(start_time) + "-" + std::to_string(end_time);
          auto reader = client->ReadObject(trace_struct_bucket, obj_name);
          if (reader.status().code() == ::google::cloud::StatusCode::kNotFound) {
            continue;
          } else if (!reader) {
            std::cerr << "Error reading object: " << reader.status() << "\n";
            return 1;
          } else {
            std::string contents{std::istreambuf_iterator<char>{reader}, {}};
            int traceID_location = contents.find(traceID);
            if (traceID_location) {
                trace_found = true;
                int end = contents.find("Trace ID", traceID_location+1);
                std::string spans = contents.substr(traceID_location, end);
                std::cout << spans << std::endl;
            }
            
          }
        }
    }

    return 0;
}

std::string extract_batch_name(std::string object_name) {
	std::vector<std::string> result;
	boost::split(result, object_name, boost::is_any_of("/"));

	return result[1];
}

std::pair<int, int> extract_batch_timestamps(std::string batch_name) {
	std::vector<std::string> result;
	boost::split(result, batch_name, boost::is_any_of("-"));
	if (result.size() != 3) {
		std::cerr << "Error in extract_batch_timestamps with batch name: " << batch_name << std::endl;
	}

	return std::make_pair(std::stoi(result[1]), std::stoi(result[2]));
}

int dummy_tests() {
	std::cout << is_object_within_timespan("12-123-125", 123, 124) << ":1" << std::endl;
	std::cout << is_object_within_timespan("12-123-125", 124, 128) << ":1" << std::endl;
	std::cout << is_object_within_timespan("12-123-125", 119, 124) << ":1" << std::endl;
	std::cout << is_object_within_timespan("12-123-125", 123, 123) << ":1" << std::endl;
	std::cout << is_object_within_timespan("12-123-125", 125, 125) << ":1" << std::endl;
	std::cout << is_object_within_timespan("12-123-125", 121, 122) << ":0" << std::endl;
	std::cout << is_object_within_timespan("12-123-125", 126, 126) << ":0" << std::endl;
	std::cout << is_object_within_timespan("12-123-125", 126, 127) << ":0" << std::endl;
	return 0;
}