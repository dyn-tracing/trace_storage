#include "graph_query.h"

std::vector<std::string> query(
    trace_structure query_trace, int start_time, int end_time,
    std::vector<query_condition> conditions, return_value ret, gcs::Client* client) {

    std::vector<query_condition*> non_indexed_conditions;

    // first, get all matches to indexed query conditions
    // note that structural is always indexed

    std::future<std::vector<traces_by_structure>> struct_filter_objs = std::async(std::launch::async,
        get_traces_by_structure,
        query_trace, start_time, end_time, client);
    std::vector<std::future<std::vector<objname_to_matching_trace_ids>>> index_results_futures;
    for (int i=0; i<conditions.size(); i++) {
        if (is_indexed(&conditions[i], client)) {
            index_results_futures.push_back(std::async(std::launch::async, get_traces_by_indexed_condition,
            start_time, end_time, &conditions[i], client));
        } else {
            non_indexed_conditions.push_back(&conditions[i]);
        }
    }

    std::vector<std::vector<objname_to_matching_trace_ids>> index_results;
    for (int i=0; i<index_results_futures.size(); i++) {
        index_results.push_back(index_results_futures[i].get());
    }
    auto struct_results = struct_filter_objs.get();

    std::vector<objname_to_matching_trace_ids> intersection = intersect_index_results(index_results, struct_results);

    std::vector<objname_to_matching_trace_ids> filtered = filter_based_on_non_indexed_conditions(
        intersection, non_indexed_conditions, client);

    return get_return_value(filtered, ret, client);
}

bool is_indexed(query_condition *condition, gcs::Client* client) {
    // TODO
    return false;
}

std::vector<objname_to_matching_trace_ids> get_traces_by_indexed_condition(int start_time, int end_time, query_condition *condition, gcs::Client* client) {
    // TODO
    std::vector<objname_to_matching_trace_ids> to_return;
    return to_return;

}

std::vector<objname_to_matching_trace_ids> filter_based_on_non_indexed_conditions(
        std::vector<objname_to_matching_trace_ids> intersection, std::vector<query_condition*> non_indexed_conditions, gcs::Client* client) {
    // TODO

}

std::vector<objname_to_matching_trace_ids> intersect_index_results(std::vector<std::vector<objname_to_matching_trace_ids>> index_results, std::vector<traces_by_structure> structural_results) {
    // TODO
}

std::vector<std::string> get_return_value(std::vector<objname_to_matching_trace_ids> filtered, return_value ret, gcs::Client* client) {
    // TODO
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


data_for_verifying_conditions get_gcs_objects_required_for_verifying_conditions(
	std::vector<query_condition> conditions, std::vector<std::unordered_map<int, int>> iso_maps,
	std::unordered_map<int, std::string> trace_node_names,
	std::unordered_map<int, std::string> query_node_names,
	std::string batch_name, std::string trace, gcs::Client* client
) {
	data_for_verifying_conditions response;
	std::vector<std::pair<std::string, std::future<opentelemetry::proto::trace::v1::TracesData>>> response_futures;

	for (auto curr_condition : conditions) {
		std::vector <std::string> iso_map_to_service;

		for (auto curr_iso_map : iso_maps) {
			auto trace_node_index = curr_iso_map[curr_condition.node_index];
			auto condition_service = trace_node_names[trace_node_index];
			iso_map_to_service.push_back(condition_service);

			auto service_name_without_hash_id = split_by_char(condition_service, ":")[0];
			if (response.service_name_to_respective_object.find(
				service_name_without_hash_id) == response.service_name_to_respective_object.end()
			) {
				response_futures.push_back(std::make_pair(service_name_without_hash_id, std::async(
					std::launch::async, read_object_and_parse_traces_data,
					service_name_without_hash_id + SERVICES_BUCKETS_SUFFIX, batch_name, client)));
			}
		}

		response.service_name_for_condition_with_isomap.push_back(iso_map_to_service);
	}

	for_each(response_futures.begin(), response_futures.end(),
		[&response](std::pair<std::string, std::future<opentelemetry::proto::trace::v1::TracesData>>& fut) {
			response.service_name_to_respective_object[fut.first] = fut.second.get();
	});

	return response;
}

std::vector<std::string> filter_trace_ids_based_on_conditions(
	std::vector<std::string> trace_ids,
	int trace_ids_start_index,
	std::string object_content,
	std::vector<query_condition> conditions,
	int num_iso_maps,
	data_for_verifying_conditions& required_data
) {
	std::vector<std::string> response;

	for (int i = trace_ids_start_index; (i < trace_ids.size() && i < trace_ids_start_index + 100); i++) {
		auto current_trace_id = trace_ids[i];
		bool satisfies_conditions = does_trace_satisfy_all_conditions(
			current_trace_id, object_content, conditions, num_iso_maps, required_data);

		if (true == satisfies_conditions) {
			response.push_back(current_trace_id);
		}
	}

	return response;
}

bool does_trace_satisfy_all_conditions(
	std::string trace_id, std::string object_content, std::vector<query_condition> conditions,
	int num_iso_maps, data_for_verifying_conditions& verification_data
) {
	std::vector<std::future<std::vector<int>>> response_futures;

	for (int curr_cond_ind = 0; curr_cond_ind < conditions.size(); curr_cond_ind++) {
		response_futures.push_back(std::async(
			std::launch::async,
			get_iso_maps_indices_for_which_trace_satifies_condition,
			trace_id, conditions[curr_cond_ind], num_iso_maps,
			object_content, std::ref(verification_data), curr_cond_ind));
	}

	std::unordered_map<int, int> iso_map_to_num_of_satisfied_conditions;
	for (int i = 0; i < num_iso_maps; i++) {
		iso_map_to_num_of_satisfied_conditions[i] = 0;
	}

	for(int i = 0; i < response_futures.size(); i++) {
		auto satisfying_iso_map_indices = response_futures[i].get();
		for (auto& iso_map_ind : satisfying_iso_map_indices) {
			iso_map_to_num_of_satisfied_conditions[iso_map_ind] += 1;
		}
	}

	for (int i = 0; i < num_iso_maps; i++) {
		if (iso_map_to_num_of_satisfied_conditions[i] >= conditions.size()) {
			return true;
		}
	}

	return false;
}


std::vector<int> get_iso_maps_indices_for_which_trace_satifies_condition(
	std::string trace_id, query_condition condition,
	int num_iso_maps, std::string object_content,
	data_for_verifying_conditions& verification_data, int condition_index_in_verification_data
) {
	std::vector<int> satisfying_iso_map_indices;
	for (int curr_iso_map_ind = 0; curr_iso_map_ind < num_iso_maps; curr_iso_map_ind++) {
		auto trace = extract_trace_from_traces_object(trace_id, object_content);
		auto trace_lines = split_by_line(trace);

		auto condition_service = verification_data.service_name_for_condition_with_isomap[
			condition_index_in_verification_data][curr_iso_map_ind];
		for (auto line : trace_lines) {
			if (line.find(condition_service) != std::string::npos) {
				auto span_info = split_by_char(line, ":");
				auto span_id = span_info[1];
				auto service_name = span_info[2];
				if (true == does_span_satisfy_condition(span_id, service_name, condition, verification_data)) {
					satisfying_iso_map_indices.push_back(curr_iso_map_ind);
				}
				break;
			}
		}
	}

	return satisfying_iso_map_indices;
}

bool does_span_satisfy_condition(
	std::string span_id, std::string service_name,
	query_condition condition, data_for_verifying_conditions& verification_data
) {
	opentelemetry::proto::trace::v1::TracesData* trace_data = &(
		verification_data.service_name_to_respective_object[service_name]);

	const opentelemetry::proto::trace::v1::Span* sp;
	for (int i=0; i < trace_data->resource_spans(0).scope_spans(0).spans_size(); i++) {
		sp = &(trace_data->resource_spans(0).scope_spans(0).spans(i));

		std::string current_span_id = hex_str(sp->span_id(), sp->span_id().length());
		if (current_span_id == span_id) {
            return does_condition_hold(sp, condition);
		}
	}

	return false;
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
	// a.node_names.insert(std::make_pair(1, "NONE"));
	// a.node_names.insert(std::make_pair(2, "c"));

	// a.edges.insert(std::make_pair(0, 1));
	// a.edges.insert(std::make_pair(1, 2));

	// trace_structure b;
	// b.num_nodes = 4;
	// b.node_names.insert(std::make_pair(0, "a"));
	// b.node_names.insert(std::make_pair(1, "b"));
	// b.node_names.insert(std::make_pair(2, "c"));
	// b.node_names.insert(std::make_pair(3, "d"));

	// b.edges.insert(std::make_pair(0, 1));
	// b.edges.insert(std::make_pair(0, 3));
	// b.edges.insert(std::make_pair(1, 2));
	// b.edges.insert(std::make_pair(3, 2));

	// std::cout << get_isomorphism_mappings(a, b) << std::endl;

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

	// auto client = gcs::Client();
	// for (auto&& item : client.ListObjectsAndPrefixes(
    //          TRACE_HASHES_BUCKET, gcs::Delimiter("/"))) {
    //   if (!item) throw std::runtime_error(item.status().message());
    //   auto result = *std::move(item);
    //   if (absl::holds_alternative<gcs::ObjectMetadata>(result)) {
    //     std::cout << "object_name="
    //               << absl::get<gcs::ObjectMetadata>(result).name() << "\n";
    //   } else if (absl::holds_alternative<std::string>(result)) {
    //     std::cout << "prefix     =" << absl::get<std::string>(result) << "\n";
    //   }
    // }
	// exit(1);
	return 0;
}
