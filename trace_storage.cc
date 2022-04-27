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

	// Building a query trace where [frontend]-()->[]-()->[emailservice]-()
	trace_structure query_trace;
	query_trace.num_nodes = 2;
	query_trace.node_names.insert(std::make_pair(0, "frontend"));
	query_trace.node_names.insert(std::make_pair(1, ASTERISK_SERVICE));

	query_trace.edges.insert(std::make_pair(0, 1));

	auto client = gcs::Client();
	int total = get_traces_by_structure(query_trace, 1550574225, 1750574225, &client);
	std::cout << "Total results: " << total << std::endl;
	return 0;
}

/**
 * @brief Get the traces by structure. 
 * 
 * @param query_trace 
 * @param start_time 
 * @param end_time 
 * @param client 
 * @return int 
 */
int get_traces_by_structure(trace_structure query_trace, int start_time, int end_time, gcs::Client* client) {
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
		if (trace_ids.size() < 1) {
			continue;
		}

		if (false == does_trace_structure_conform_to_graph_query(batch_name, trace_ids[0], query_trace, client)) {
			std::cout << object_name << std::endl;
			continue;
		}
		
		response.insert(response.end(), trace_ids.begin(), trace_ids.end());
	}

	return response.size();
}

bool does_trace_structure_conform_to_graph_query(std::string traces_object, std::string trace_id, trace_structure query_trace, gcs::Client* client ) {
	std::string object_content = read_object(trace_struct_bucket, traces_object, client);
	std::string trace = extract_trace_from_traces_object(trace_id, object_content);

	trace_structure candidate_trace = morph_trace_object_to_trace_structure(trace);
	bool response = is_isomorphic(query_trace, candidate_trace);
	return response;
}

std::vector<std::string> split_by_line(std::string input) {
	std::vector<std::string> result = split_by(input, "\n");
	if (result[result.size()-1].length() < 1) {
		result.pop_back();
	}

	return result;
}

/**
 * @brief Right now this function only calculates whether the batch timestamps have some overlap with the 
 * timespan provided in the query. TODO: To be accurate, calculate whether the object
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

std::string extract_trace_from_traces_object(std::string trace_id, std::string object_content) {
	int start_ind = object_content.find("Trace ID: " + trace_id + ":");
	if (start_ind == std::string::npos) {
		std::cerr << "trace_id (" << trace_id << ") not found in the object_content" << std::endl;
		exit(1);
	}

	int end_ind = object_content.find("Trace ID", start_ind+1);
	if (end_ind == std::string::npos) {
		// not necessarily required as end_ind=npos does the same thing, but for clarity:
		end_ind = object_content.length() - start_ind;
	}

	std::string trace = object_content.substr(start_ind, end_ind-start_ind);
	trace = strip_from_the_end(trace, '\n');
	return trace;
}

std::string strip_from_the_end(std::string object, char stripper) {
	if (!object.empty() && object[object.length()-1] == stripper) {
		object.erase(object.length()-1);
	}
	return object;
}

trace_structure morph_trace_object_to_trace_structure(std::string trace) {
	trace_structure response;
	
	std::vector<std::string> trace_lines = split_by_line(trace);
	std::unordered_map<std::string, std::string> span_to_service;
	std::unordered_map<std::string, int> reverse_node_names;
	std::multimap<std::string, std::string> edges;

	for (auto line: trace_lines) {
		if (line.substr(0, 10) == "Trace ID: ") {
			continue;
		}

		std::vector<std::string> span_info = split_by(line, ":");
		if (span_info.size() != 3) {
			std::cerr << "Malformed trace found: \n" << trace << std::endl;
			exit(1);
		}
		
		span_to_service.insert(std::make_pair(span_info[1], span_info[2]+"-"+span_info[1]));

		if (span_info[0].length() > 0) {
			edges.insert(std::make_pair(span_info[0], span_info[1]));
		}
	}

	response.num_nodes = span_to_service.size();

	/**
	 * TODO: Make sure that the traces with multiple spans of a single
	 * service are catered properly. Seems like it is already handled
	 * properly, just mako suro!
	 */

	// Filling response.node_names
	int count = 0;
	for(const auto& elem : span_to_service) {
		response.node_names.insert(make_pair(count, split_by(elem.second, "-")[0]));
		reverse_node_names.insert(make_pair(elem.second, count));
		count++;
	}

	// Filling response.edges
	for(const auto& elem : edges) {
		response.edges.insert(std::make_pair(
			reverse_node_names[span_to_service[elem.first]],
			reverse_node_names[span_to_service[elem.second]]
		));
	}

	return response;
}

std::vector<std::string> split_by(std::string input, std::string splitter) {
	std::vector<std::string> result;
	boost::split(result, input, boost::is_any_of(splitter));
	return result;
}

void print_trace_structure(trace_structure trace) {
	std::cout << "n: " << trace.num_nodes << std::endl;
	std::cout << "node names:" << std::endl;
	for (const auto& elem : trace.node_names) {
		std::cout << elem.first << " : " << elem.second << std::endl;
	}
	std::cout << "edges:" << std::endl;
	for (const auto& elem : trace.edges) {
		std::cout << elem.first << " : " << elem.second << std::endl;
	}
}

bool is_isomorphic(trace_structure query_trace, trace_structure candidate_trace) {
	graph_type query_graph = morph_trace_structure_to_boost_graph_type(query_trace);
	graph_type candidate_graph = morph_trace_structure_to_boost_graph_type(candidate_trace);
	vertex_comp_t vertex_comp = make_property_map_equivalent_custom(boost::get(boost::vertex_name_t::vertex_name, query_graph), boost::get(boost::vertex_name_t::vertex_name, candidate_graph));
	vf2_callback_custom<graph_type, graph_type> callback(query_graph, candidate_graph);
	bool res = boost::vf2_subgraph_iso(query_graph, candidate_graph, callback, boost::vertex_order_by_mult(query_graph), boost::vertices_equivalent(vertex_comp));
	return res;
}

graph_type morph_trace_structure_to_boost_graph_type(trace_structure input_graph) {
	graph_type output_graph;

	for (int i = 0; i < input_graph.num_nodes; i++) {
		boost::add_vertex(vertex_property(input_graph.node_names[i], i), output_graph);
	}

	for (const auto& elem : input_graph.edges) {
		boost::add_edge(elem.first, elem.second, output_graph);
	}

	return output_graph;
}
int dummy_tests() {
	// std::cout << is_object_within_timespan("12-123-125", 123, 124) << ":1" << std::endl;
	// std::cout << is_object_within_timespan("12-123-125", 124, 128) << ":1" << std::endl;
	// std::cout << is_object_within_timespan("12-123-125", 119, 124) << ":1" << std::endl;
	// std::cout << is_object_within_timespan("12-123-125", 123, 123) << ":1" << std::endl;
	// std::cout << is_object_within_timespan("12-123-125", 125, 125) << ":1" << std::endl;
	// std::cout << is_object_within_timespan("12-123-125", 121, 122) << ":0" << std::endl;
	// std::cout << is_object_within_timespan("12-123-125", 126, 126) << ":0" << std::endl;
	// std::cout << is_object_within_timespan("12-123-125", 126, 127) << ":0" << std::endl;

	// std::cout << extract_trace_from_traces_object("123", "Trace ID: 1234: abcd def Trace ID: 123: thats complete! Trace ID") << std::endl;

	// auto response = morph_trace_object_to_trace_structure("Trace ID: 123:\n1:2:a\n1:3:b\n:1:f\n2:4:c\n4:5:b");

	// trace_structure a;
	// a.num_nodes = 3;
	// a.node_names.insert(std::make_pair(0, "a"));
	// a.node_names.insert(std::make_pair(1, "b"));
	// a.node_names.insert(std::make_pair(2, "c"));


	// trace_structure b;
	// b.num_nodes = 3;
	// b.node_names.insert(std::make_pair(0, "a"));
	// b.node_names.insert(std::make_pair(1, "b"));
	// b.node_names.insert(std::make_pair(2, "c"));

	// std::cout << is_isomorphic(a, b) << std::endl;
	return 0;
}
