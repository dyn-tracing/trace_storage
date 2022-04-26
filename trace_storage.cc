// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Examples: https://github.com/googleapis/google-cloud-cpp/tree/main/google/cloud/storage/examples

#include "google/cloud/storage/client.h"
#include <iostream>

const std::string trace_struct_bucket = "dyntraces-snicket2";
const std::string trace_hashes_bucket = "tracehashes-snicket2";

namespace gcs = ::google::cloud::storage;

bool does_trace_structure_conform_to_graph_query( std::string trace_id, std::string parent_service, std::string child_service, gcs::Client* client);
std::vector<std::string> split_by_line(const std::string& str);
bool is_object_within_timespan(std::string object_name, int start_time, int end_time);
std::string read_object(std::string bucket, std::string object, gcs::Client* client);
std::vector<std::string> get_trace_ids_from_trace_hashes_object(std::string object_name, gcs::Client* client);
int get_trace(std::string traceID, int start_time, int end_time, gcs::Client* client); 
int get_traces_by_structure(std::string parent_service, std::string child_service, int start_time, int end_time, gcs::Client* client);

int main(int argc, char* argv[]) {
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
	// return get_trace("d64c8acc182c277ac6f78620bca62310", 1650574264, 1650574264, &client);
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

		if (false == is_object_within_timespan(object_name, start_time, end_time)) {
			continue;
		}

		std::vector<std::string> trace_ids = get_trace_ids_from_trace_hashes_object(object_name, client);
		if (trace_ids.size() < 1) {
			continue;
		}

		if (false == does_trace_structure_conform_to_graph_query(trace_ids[0], parent_service, child_service, client)) {
			continue;
		}
		
		response.insert(response.end(), trace_ids.begin(), trace_ids.end());

		break;
	}

	return response.size();
}

bool does_trace_structure_conform_to_graph_query( std::string trace_id, std::string parent_service, std::string child_service, gcs::Client* client ) {
	/** 
	 * TODO: 
	 * */
	return true;
}

std::vector<std::string> split_by_line(const std::string& str) {
	std::vector<std::string> tokens;

	std::string::size_type pos = 0;
	std::string::size_type prev = 0;
	std::string ele;

	while ((pos = str.find('\n', prev)) != std::string::npos) {
		ele = str.substr(prev, pos - prev);
		if (ele.length() > 0) {
			tokens.push_back(ele);
		}
		prev = pos + 1;
	}

	ele = str.substr(prev);
	if (ele.length() > 0) {
		tokens.push_back(ele);
	}

	return tokens;
}

bool is_object_within_timespan(std::string object_name, int start_time, int end_time) {
	/** 
	 * TODO: 
	 * */
	return true;
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