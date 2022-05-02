// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#include "graph_query.h"

int main(int argc, char* argv[]) {
	dummy_tests();

	// Building a query trace where [frontend]-()->[]-()->[emailservice]-()
	trace_structure query_trace;
	query_trace.num_nodes = 3;
	query_trace.node_names.insert(std::make_pair(0, "frontend"));
	query_trace.node_names.insert(std::make_pair(1, ASTERISK_SERVICE));
	query_trace.node_names.insert(std::make_pair(2, "emailservice"));

	query_trace.edges.insert(std::make_pair(0, 1));
	query_trace.edges.insert(std::make_pair(1, 2));

	auto client = gcs::Client();
	std::vector<std::string> total = get_traces_by_structure(query_trace, 1650574275, 1650574275, &client);
	std::cout << "Total results: " << total.size() << std::endl;
	// for (std::string i: total) {
	// 	std::cout << i << std::endl;
	// }
	return 0;
}

/**
 * @brief Get the traces by structure. 
 * 
 * @param query_trace 
 * @param start_time 
 * @param end_time 
 * @param client 
 * @return std::vector<std::string> 
 */
std::vector<std::string> get_traces_by_structure(trace_structure query_trace, int start_time, int end_time, gcs::Client* client) {
	std::vector<std::string> response;

	for (auto&& object_metadata : client->ListObjects(TRACE_HASHES_BUCKET)) {
		response_futures.push_back(std::async(
			std::launch::async, process_trace_hashes_object_and_retrieve_relevant_trace_ids,
			object_metadata, query_trace, start_time, end_time, client));
	}

	return response;
}

std::vector<std::string> process_trace_hashes_object_and_retrieve_relevant_trace_ids(
	StatusOr<gcs::ObjectMetadata> object_metadata,
	trace_structure query_trace,
	int start_time,
	int end_time,
	gcs::Client* client
) {
	if (!object_metadata) {
		std::cerr << object_metadata.status().message() << std::endl;
		exit(1);
	}

	std::vector<std::string> response_trace_ids;

	std::string object_name = object_metadata->name();
	std::string batch_name = extract_batch_name(object_name);

	std::pair<int, int> batch_time = extract_batch_timestamps(batch_name);
	if (false == is_object_within_timespan(batch_time, start_time, end_time)) {
		return response_trace_ids;
	}

	response_trace_ids = get_trace_ids_from_trace_hashes_object(object_name, client);
	if (response_trace_ids.size() < 1) {
		return response_trace_ids;
	}

	std::string object_content = read_object(TRACE_STRUCT_BUCKET, batch_name, client);

	if (false == does_trace_structure_conform_to_graph_query(object_content, response_trace_ids[0], query_trace, client)) {
		return response_trace_ids;
	}

	response_trace_ids = filter_trace_ids_based_on_query_timestamp(
		response_trace_ids, batch_name, object_content, start_time, end_time, client);
	return response_trace_ids;
}

/**
 * TODO: maybe pass a pointer to object_content only, would that be more performant?
 */
bool does_trace_structure_conform_to_graph_query(
	std::string object_content, std::string trace_id, trace_structure query_trace, gcs::Client* client ) {
	std::string trace = extract_trace_from_traces_object(trace_id, object_content);

	trace_structure candidate_trace = morph_trace_object_to_trace_structure(trace);
	bool response = is_isomorphic(query_trace, candidate_trace);
	return response;
}

std::vector<std::string> split_by_line(std::string input) {
	std::vector<std::string> result = split_by_char(input, "\n");
	if (result[result.size()-1].length() < 1) {
		result.pop_back();
	}

	return result;
}

/**
 * @brief Right now this function only calculates whether the batch timestamps have some overlap with the 
 * timespan provided in the query. 
 * 
 * @return bool
 */
bool is_object_within_timespan(std::pair<int, int> batch_time, int start_time, int end_time) {
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
		std::cerr << "Error reading object " << bucket << "/" << object << " :" << reader.status() << "\n";
		exit(1);
	}

	std::string object_content{std::istreambuf_iterator<char>{reader}, {}};
	return object_content;
}

std::vector<std::string> get_trace_ids_from_trace_hashes_object(std::string object_name, gcs::Client* client) {
	std::string object_content = read_object(TRACE_HASHES_BUCKET, object_name, client);
	std::vector<std::string> trace_ids = split_by_line(object_content);

	return trace_ids;
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

	for (auto line : trace_lines) {
		if (line.substr(0, 10) == "Trace ID: ") {
			continue;
		}

		std::vector<std::string> span_info = split_by_char(line, ":");
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
		response.node_names.insert(make_pair(count, split_by_char(elem.second, "-")[0]));
		reverse_node_names.insert(make_pair(elem.second, count));
		count++;
	}

	// Filling response.edges
	for(const auto& elem : edges) {
		response.edges.insert(std::make_pair(
			reverse_node_names[span_to_service[elem.first]],
			reverse_node_names[span_to_service[elem.second]]));
	}

	return response;
}

std::vector<std::string> split_by_string(std::string input, std::string splitter) {
	std::vector<std::string> result;

	size_t pos = 0;
	std::string token;
	while ((pos = input.find(splitter)) != std::string::npos) {
		token = input.substr(0, pos);
		token = strip_from_the_end(token, '\n');
		if (token.length() > 0) {
			result.push_back(token);
		}
		input.erase(0, pos + splitter.length());
	}

	input = strip_from_the_end(input, '\n');
	if (input.length() > 0) {
		result.push_back(input);
	}

	return result;
}

std::vector<std::string> split_by_char(std::string input, std::string splitter) {
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
	vertex_comp_t vertex_comp = make_property_map_equivalent_custom(
		boost::get(boost::vertex_name_t::vertex_name, query_graph),
		boost::get(boost::vertex_name_t::vertex_name, candidate_graph));
	vf2_callback_custom<graph_type, graph_type> callback(query_graph, candidate_graph);
	bool res = boost::vf2_subgraph_iso(
		query_graph,
		candidate_graph,
		callback,
		boost::vertex_order_by_mult(query_graph),
		boost::vertices_equivalent(vertex_comp));
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

/**
 * @brief This is the place to do filtering of the trace_ids. RN I am filetering just on the start and end timestamp but it can
 * be generalized to cater to other filtering objectives. 
 * 
 * @param trace_ids 
 * @param batch_name 
 * @param object_content 
 * @param start_time 
 * @param end_time 
 * @return std::vector<std::string> 
 */
std::vector<std::string> filter_trace_ids_based_on_query_timestamp(
	std::vector<std::string> trace_ids,
	std::string batch_name,
	std::string object_content,
	int start_time,
	int end_time,
	gcs::Client* client) {
	std::vector<std::string> response;

	std::map<std::string, std::string> trace_id_to_root_service_map = get_trace_id_to_root_service_map(object_content);
	std::map<std::string, std::vector<std::string>> root_service_to_trace_ids_map = get_root_service_to_trace_ids_map(
		trace_id_to_root_service_map);

	for (auto const& elem : root_service_to_trace_ids_map) {
		std::string bucket = elem.first + SERVICES_BUCKETS_SUFFIX;
		std::string spans_data = read_object(bucket, batch_name, client);

		std::map<std::string, std::pair<int, int>>
		trace_id_to_timestamp_map = get_timestamp_map_for_trace_ids(spans_data, trace_ids);

		std::vector<std::string> successful_trace_ids;
		for(auto const& trace_id : elem.second) {
			std::pair<int, int> trace_timestamp = trace_id_to_timestamp_map[trace_id];
			if (is_object_within_timespan(trace_timestamp, start_time, end_time)) {
				successful_trace_ids.push_back(trace_id);
			}
		}

		response.insert(response.end(), successful_trace_ids.begin(), successful_trace_ids.end());
	}

	return response;
}

std::string hex_str(std::string data, int len) {
	constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

	std::string s(len * 2, ' ');
	for (int i = 0; i < len; ++i) {
		s[2 * i]     = hexmap[(data[i] & 0xF0) >> 4];
		s[2 * i + 1] = hexmap[data[i] & 0x0F];
	}

	return s;
}

std::map<std::string, std::pair<int, int>> get_timestamp_map_for_trace_ids(
	std::string spans_data, std::vector<std::string> trace_ids) {
	std::map<std::string, std::pair<int, int>> response;

	opentelemetry::proto::trace::v1::TracesData trace_data;
	bool ret = trace_data.ParseFromString(spans_data);
	if (false == ret) {
		std::cerr << "Error in ParseFromString" << std::endl;
		exit(1);
	}

	for (int i=0; i < trace_data.resource_spans(0).scope_spans(0).spans_size(); i++) {
		opentelemetry::proto::trace::v1::Span sp = trace_data.resource_spans(0).scope_spans(0).spans(i);

		std::string trace_id = hex_str(sp.trace_id(), sp.trace_id().length());

		// getting timestamps and converting from nanosecond precision to seconds precision
		int start_time = std::stoi(std::to_string(sp.start_time_unix_nano()).substr(0, 10));
		int end_time = std::stoi(std::to_string(sp.end_time_unix_nano()).substr(0, 10));

		response.insert(std::make_pair(trace_id, std::make_pair(start_time, end_time)));
	}

	return response;
}

std::map<std::string, std::string> get_trace_id_to_root_service_map(std::string object_content) {
	std::map<std::string, std::string> response;
	std::vector<std::string> all_traces = split_by_string(object_content, "Trace ID: ");

	for (std::string i : all_traces) {
		std::vector<std::string> trace = split_by_char(i, "\n");
		std::string trace_id = trace[0].substr(0, TRACE_ID_LENGTH);
		for (int ind = 1; ind < trace.size(); ind ++) {
			if (trace[ind].substr(0, 1) == ":") {
				std::vector<std::string> root_span_info = split_by_char(trace[ind], ":");
				std::string root_service = root_span_info[root_span_info.size()-1];
				response.insert(std::make_pair(trace_id, root_service));
				break;
			}
		}
	}

	return response;
}

std::map<std::string, std::vector<std::string>> get_root_service_to_trace_ids_map(
	std::map<std::string, std::string> trace_id_to_root_service_map) {
	std::map<std::string, std::vector<std::string>> response;

	for (auto const& elem : trace_id_to_root_service_map) {
		response[elem.second].push_back(elem.first);
	}

	return response;
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

	// std::cout << extract_trace_from_traces_object("123",
	// "Trace ID: 1234: abcd def Trace ID: 123: thats complete! Trace ID") << std::endl;

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

	// std::map<std::string, std::string> m;
	// m["A"] = "B";
	// m["B"] = "B";
	// m["C"] = "B";
	// m["D"] = "B";
	// m["AA"] = "BB";
	// m["BB"] = "BB";
	// m["CC"] = "BB";
	// m["DD"] = "BB";
	// std::map<std::string, std::vector<std::string>> res = get_root_service_to_trace_ids_map(m);
	// for (auto const& elem : res) {
	// 	std::cout << elem.first << ": ";
	// 	for (auto const& vec_elem: elem.second) {
	// 		std::cout << vec_elem << ",";
	// 	}
	// 	std::cout << std::endl;
	// }

	// exit(1);
	return 0;
}
