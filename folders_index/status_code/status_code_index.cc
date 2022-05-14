// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#include "status_code_index.h"

int main(int argc, char* argv[]) {
	dummy_tests();

	auto client = gcs::Client();
	time_t last_updated = 0;
	std::string indexed_attribute = ATTR_SPAN_KIND;
	std::string attribute_value = "server";

	boost::posix_time::ptime start, stop;
	start = boost::posix_time::microsec_clock::local_time();
	update_index(&client, last_updated, indexed_attribute, attribute_value);
	stop = boost::posix_time::microsec_clock::local_time();

	boost::posix_time::time_duration dur = stop - start;
	int64_t milliseconds = dur.total_milliseconds();
	std::cout << "Time taken: " << milliseconds << std::endl;
	return 0;
}

int get_total_of_trace_ids(std::unordered_map<std::string, std::vector<std::string>> attr_to_trace_ids) {
	int count = 0;
	for (auto& ele : attr_to_trace_ids) {
		count += ele.second.size();
	}
	return count;
}

int update_index(gcs::Client* client, time_t last_updated, 
	std::string indexed_attribute, std::string attribute_value
) {
	create_index_bucket_if_not_present(indexed_attribute, client);

	std::vector<std::string> span_buckets_names = get_spans_buckets_names(client);
	std::vector<std::string> trace_struct_object_names = get_all_object_names(TRACE_STRUCT_BUCKET, client);
	trace_struct_object_names = sort_object_names_on_start_time(trace_struct_object_names);

	std::vector<
		std::pair<
			std::string,
			std::future<
				std::unordered_map<
					std::string, 
					std::vector<
						std::string>>>>> response_futures;

	for (auto object_name : trace_struct_object_names) {
		response_futures.push_back(std::make_pair(object_name, std::async(std::launch::async,
			get_attr_to_trace_ids_map, object_name, indexed_attribute, attribute_value,
			std::ref(span_buckets_names), client)));
		break;
	}

	index_batch current_index_batch = index_batch();
	for (int i = 0; i < response_futures.size(); i++) {
		auto object_name = response_futures[i].first;
		auto attr_to_trace_ids_map = response_futures[i].second.get();

		current_index_batch.trace_ids_with_timestamps.push_back(std::make_pair(object_name, attr_to_trace_ids_map));

		auto to_export = get_attr_vals_which_have_enough_data_to_export(current_index_batch);

		if (to_export.size() > 0) {
			export_batch_to_storage(current_index_batch, indexed_attribute, attribute_value, to_export, client);
			current_index_batch = index_batch();
		}
	}

	export_batch_to_storage(current_index_batch, indexed_attribute, attribute_value, {}, client);
	return 0;
}

std::vector<std::string> get_spans_buckets_names(gcs::Client* client) {
	std::vector<std::string> response;

	for (auto&& bucket_metadata : client->ListBucketsForProject(PROJECT_ID)) {
		if (!bucket_metadata) {
			std::cerr << bucket_metadata.status().message() << std::endl;
			exit(1);
		}

		if (true == bucket_metadata->labels().empty()) {
			continue;
		}

		for (auto const& kv : bucket_metadata->labels()) {
			if (kv.first == BUCKET_TYPE_LABEL_KEY && kv.second == BUCKET_TYPE_LABEL_VALUE_FOR_SPAN_BUCKETS) {
				response.push_back(bucket_metadata->name());
			}
		}
	}

	return response;
}

std::vector<std::string> get_all_object_names(std::string bucket_name, gcs::Client* client) {
	std::vector<std::string> response;

	for (auto&& object_metadata : client->ListObjects(bucket_name)) {
		if (!object_metadata) {
			std::cerr << object_metadata.status().message() << std::endl;
			exit(1);
		}

		response.push_back(object_metadata->name());
	}

	return response;
}

std::vector<std::string> split_by_char(std::string input, std::string splitter) {
	std::vector<std::string> result;
	boost::split(result, input, boost::is_any_of(splitter));
	return result;
}


bool compare_object_names_by_start_time(std::string object_name1, std::string object_name2) {
	// Object name format is somehash-starttime-endtime
	return std::stol(split_by_char(object_name1, "-")[1]) < std::stol(split_by_char(object_name2, "-")[1]);
}

std::vector<std::string> sort_object_names_on_start_time(std::vector<std::string> object_names) {
	sort(object_names.begin(), object_names.end(), compare_object_names_by_start_time);
	return object_names;	
}

std::unordered_map<std::string, std::vector<std::string>> get_attr_to_trace_ids_map(
	std::string object_name, std::string indexed_attribute, std::string attribute_value,
	std::vector<std::string>& span_buckets_names, gcs::Client* client
) {
	std::unordered_map<std::string, std::vector<std::string>> attr_to_trace_ids_map;

	for (auto span_bucket : span_buckets_names) {
		std::unordered_map<std::string, std::vector<std::string>> local_attr_to_trace_ids_map = calculate_attr_to_trace_ids_map_for_microservice(
			span_bucket, object_name, indexed_attribute, attribute_value, client);

		take_per_field_union(attr_to_trace_ids_map, local_attr_to_trace_ids_map);
	}

	return attr_to_trace_ids_map;
}

void take_per_field_union(std::unordered_map<std::string, std::vector<std::string>>& attr_to_trace_ids_map,
	std::unordered_map<std::string, std::vector<std::string>>& local_attr_to_trace_ids_map
) {
	for (auto& i : local_attr_to_trace_ids_map) {
		auto local_attribute = i.first;
		auto local_trace_ids = &(i.second);

		if (attr_to_trace_ids_map.find(local_attribute) == attr_to_trace_ids_map.end()) {
			std::vector<std::string> vec;
			attr_to_trace_ids_map[local_attribute] = vec;
		}

		auto* previous_trace_ids = &(attr_to_trace_ids_map[local_attribute]);
		for (int trace_id_ind = 0; trace_id_ind < local_trace_ids->size(); trace_id_ind++) {
			if (std::find(
					previous_trace_ids->begin(),
					previous_trace_ids->end(),
					(*local_trace_ids)[trace_id_ind]
				) == previous_trace_ids->end()
			) {
				attr_to_trace_ids_map[local_attribute].push_back((*local_trace_ids)[trace_id_ind]);
			}
		}
	}
}

std::unordered_map<std::string, std::vector<std::string>> calculate_attr_to_trace_ids_map_for_microservice(
	std::string span_bucket_name, std::string object_name, std::string indexed_attribute,
	std::string attribute_value, gcs::Client* client
) {
	std::unordered_map<std::string, std::vector<std::string>> response; // attr_val_to_vec_of_traceids
	std::string raw_span_bucket_obj_content = read_object(span_bucket_name, object_name, client);
	if (raw_span_bucket_obj_content.length() < 1) {
		return response;
	}

	opentelemetry::proto::trace::v1::TracesData trace_data;
	bool ret = trace_data.ParseFromString(raw_span_bucket_obj_content);
	if (false == ret) {
		std::cerr << "Error in calculate_attr_to_trace_ids_map:ParseFromString" << std::endl;
		exit(1);
	}

	const opentelemetry::proto::trace::v1::Span* sp;
	for (int i=0; i < trace_data.resource_spans(0).scope_spans(0).spans_size(); i++) {
		sp = &(trace_data.resource_spans(0).scope_spans(0).spans(i));

		std::string trace_id = hex_str(sp->trace_id(), sp->trace_id().length());

		const opentelemetry::proto::common::v1::KeyValue* attribute;
		for (int j=0; j < sp->attributes_size(); j++) {
			attribute =  &(sp->attributes(j));
			const opentelemetry::proto::common::v1::AnyValue* val = &(attribute->value());
			auto curr_attr_key = attribute->key();
			auto curr_attr_val = val->string_value();

			if (indexed_attribute == curr_attr_key) {
				response[curr_attr_val].push_back(trace_id);
			}
		}
	}

	return response;
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

/**
 * TODO: Can do better here. 
 */
std::vector<std::string> get_attr_vals_which_have_enough_data_to_export(index_batch& current_index_batch) {
	std::unordered_map<std::string, int> attr_to_trace_ids_total;
 
	for (auto& p : current_index_batch.trace_ids_with_timestamps) {
		for (auto& map_ele : p.second) {
			if (attr_to_trace_ids_total.find(map_ele.first) == attr_to_trace_ids_total.end()) {
				attr_to_trace_ids_total[map_ele.first] = map_ele.second.size();
			} else {
				attr_to_trace_ids_total[map_ele.first] += map_ele.second.size();
			}
		}
	}

	std::vector<std::string> big_enough_attrs;

	for (auto& map_ele : attr_to_trace_ids_total) {
		if (map_ele.second*32 >= (1000000 - 100)) {
			big_enough_attrs.push_back(map_ele.first);
		}
	}

	return big_enough_attrs;
}

void print_index_batch(index_batch& current_index_batch) {
	for (auto p : current_index_batch.trace_ids_with_timestamps) {
		std::cout << p.first << " : " << std::endl;
		for (auto m : p.second) {
			std::cout << m.first << " => ";
			for (auto a : m.second) {
				std:: cout << a << " " << std::flush;
			}
			std::cout << std::endl;
		}
		std::cout << std::endl;
	}
}

/**
 * If attrs_to_export is empty, then all of them will be exported.
 */
void export_batch_to_storage(index_batch& current_index_batch, std::string indexed_attribute,
	std::string attribute_value, std::vector<std::string> attrs_to_export, gcs::Client* client
) {

	print_index_batch(current_index_batch);
	/**
	 * TODO: Implement it. 
	 * 
	 */

	// batch_timestamp consiledated_timestamp = batch_timestamp();
	// std::string object_to_write = "";

	// for (auto& elem : current_index_batch.trace_ids_with_timestamps) {
	// 	auto curr_timestamp = extract_batch_timestamps(elem.first);
	// 	auto attr_to_trace_ids_map = elem.second;

	// 	if (consiledated_timestamp.start_time == "" || 
	// 		(std::stol(curr_timestamp.start_time) < std::stol(consiledated_timestamp.start_time))
	// 	) {
	// 		consiledated_timestamp.start_time = curr_timestamp.start_time;
	// 	}

	// 	if (consiledated_timestamp.end_time == "" || 
	// 		(std::stol(curr_timestamp.end_time) > std::stol(consiledated_timestamp.end_time))
	// 	) {
	// 		consiledated_timestamp.end_time = curr_timestamp.end_time;
	// 	}

	// 	auto serialized_trace_ids = serialize_trace_ids(curr_trace_ids);
	// 	auto serialized_timestamp = "Timestamp: " + elem.first;

	// 	object_to_write += (serialized_timestamp + "\n" + serialized_trace_ids);
	// }

	// std::string autoscaling_hash = get_autoscaling_hash_from_start_time(consiledated_timestamp.start_time);

	// auto bucket_name = get_bucket_name_for_attr(indexed_attribute);
	// auto folder_name = get_folder_name_from_attr_value(attribute_value);

	// std::string object_name = folder_name + "/" + autoscaling_hash + "-" + \
	// 	consiledated_timestamp.start_time + "-" + consiledated_timestamp.end_time;

	// write_object(bucket_name, object_name, object_to_write, client);
	// return;
}

void write_object(std::string bucket_name, std::string object_name,
	std::string& object_to_write, gcs::Client* client
) {
	std::cout << "Writing " << bucket_name << "/" << object_name << std::endl;
	gcs::ObjectWriteStream stream = client->WriteObject(bucket_name, object_name);
	stream << object_to_write << "\n";
    stream.Close();

	StatusOr<gcs::ObjectMetadata> metadata = std::move(stream).metadata();
	if (!metadata) {
		throw std::runtime_error(metadata.status().message());
		exit(1);
	}
}

std::string get_autoscaling_hash_from_start_time(std::string start_time) {
	auto autoscaling_hash = std::hash<std::string>()(start_time);
	return std::to_string(autoscaling_hash).substr(0, 2);
}

std::string serialize_trace_ids(std::vector<std::string>& trace_ids) {
	std::string response = "";
	for (auto& i : trace_ids) {
		response += (i + "\n");
	}

	return response;
}

std::string read_object(std::string bucket, std::string object, gcs::Client* client) {
	auto reader = client->ReadObject(bucket, object);
	if (!reader) {
		if (reader.status().code() == ::google::cloud::StatusCode::kNotFound) {
			return "";
		}

		std::cerr << "Error reading object " << bucket << "/" << object << " :" << reader.status() << "\n";
		exit(1);
	}

	std::string object_content{std::istreambuf_iterator<char>{reader}, {}};
	return object_content;
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

std::string strip_from_the_end(std::string object, char stripper) {
	if (!object.empty() && object[object.length()-1] == stripper) {
		object.erase(object.length()-1);
	}
	return object;
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

void create_index_bucket_if_not_present(std::string indexed_attribute, gcs::Client* client) {
	auto bucket_name = get_bucket_name_for_attr(indexed_attribute);

	auto bucket_metadata = client->CreateBucketForProject(bucket_name, PROJECT_ID,
		gcs::BucketMetadata().set_location(BUCKETS_LOCATION).set_storage_class(gcs::storage_class::Regional()));

	if (bucket_metadata.status().code() == ::google::cloud::StatusCode::kAborted) {
		// ignore this, means we've already created the bucket
	} else if (!bucket_metadata) {
		std::cerr << "Error creating bucket " << bucket_name << ", status=" << bucket_metadata.status() << "\n";
		exit(1);
	}
}

int dummy_tests() {
	// std::unordered_map<std::string, std::vector<std::string>> global;
	// global["t1"] = {"a", "b"};
	// global["t2"] = {"a"};
	// global["t5"] = {"c"};

	// std::unordered_map<std::string, std::vector<std::string>> local;
	// local["t1"] = {"a", "c", "c", "a"};
	// local["t2"] = {"a"};
	// local["t6"] = {"f"};

	// take_per_field_union(global, local);

	// for (auto i : global) {
	// 	std::cout << i.first << " => ";
	// 	for (auto j : i.second) {
	// 		std::cout << j << " ";
	// 	}
	// 	std::cout << std::endl;
	// }

	// std::size_t hash = std::hash<std::string>()("foo");	
	// std::cout << std::to_string(hash).substr(0, 2) << std::endl;
	// exit(1);
	return 0;
}