// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#include "status_code_index.h"

int main(int argc, char* argv[]) {
	dummy_tests();

	auto client = gcs::Client();
	time_t last_updated = 0;
	std::string indexed_attribute = ATTR_SPAN_KIND;
	std::string attribute_value = "server";

	return update_index(&client, last_updated, indexed_attribute, attribute_value);
}

int update_index(gcs::Client* client, time_t last_updated, 
	std::string indexed_attribute, std::string attribute_value
) {
	std::vector<std::string> span_buckets_names = get_spans_buckets_names(client);
	/**
	 * TODO: (i) Following is a bad thing to do. what if all object names do not fit in the memory. 
	 * (ii) do the error handling for the case when bucket is not present. 
	 */
	std::vector<std::string> trace_struct_object_names = get_all_object_names(TRACE_STRUCT_BUCKET, client);
	trace_struct_object_names = sort_object_names_on_start_time(trace_struct_object_names);
	index_batch current_index_batch = index_batch();

	for (auto object_name : trace_struct_object_names) {
		std::vector<std::string> trace_ids_with_attribute = get_trace_ids_with_attribute(
			object_name, indexed_attribute, attribute_value, span_buckets_names, client);

		current_index_batch.total_trace_ids += trace_ids_with_attribute.size();
		current_index_batch.trace_ids_with_timestamps.push_back(std::make_pair(
			extract_batch_timestamps(object_name), trace_ids_with_attribute
		));

		if (true == is_batch_big_enough(current_index_batch)) {
			export_batch_to_storage(current_index_batch, indexed_attribute, attribute_value, client);
			current_index_batch = index_batch();
		}
		break;  // TODO remove it
	}

	export_batch_to_storage(current_index_batch, indexed_attribute, attribute_value, client);
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

		const auto label_map = bucket_metadata->labels();
		if (label_map.at(BUCKET_TYPE_LABEL_KEY) == BUCKET_TYPE_LABEL_VALUE_FOR_SPAN_BUCKETS) {
			response.push_back(bucket_metadata->name());
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

std::vector<std::string> get_trace_ids_with_attribute(
	std::string object_name, std::string indexed_attribute, std::string attribute_value,
	std::vector<std::string>& span_buckets_names, gcs::Client* client
) {
	std::unordered_map<std::string, bool> trace_id_to_attribute_membership;

	for (auto span_bucket : span_buckets_names) {
		std::unordered_map<std::string, bool> local_trace_id_to_attribute_membership = calculate_trace_id_to_attribute_map(
			span_bucket, object_name, indexed_attribute, attribute_value, client);

		take_per_field_OR(trace_id_to_attribute_membership, local_trace_id_to_attribute_membership);
	}

	std::vector<std::string> response;
	for (auto& i : trace_id_to_attribute_membership) {
		if (true == i.second) {
			response.push_back(i.first);
		}
	}

	return response;
}

void take_per_field_OR(std::unordered_map<std::string, bool>& trace_id_to_attribute_membership,
	std::unordered_map<std::string, bool>& local_trace_id_to_attribute_membership) {
	for (auto& i : local_trace_id_to_attribute_membership) {
		if ((trace_id_to_attribute_membership.find(i.first) == trace_id_to_attribute_membership.end())
			|| (false == trace_id_to_attribute_membership[i.first]) 
		) {
			trace_id_to_attribute_membership[i.first] = i.second;
		}
	}
}

std::unordered_map<std::string, bool> calculate_trace_id_to_attribute_map(std::string span_bucket_name,
	std::string object_name, std::string indexed_attribute, std::string attribute_value, gcs::Client* client
) {
	std::string raw_span_bucket_obj_content = read_object(span_bucket_name, object_name, client);
	std::unordered_map<std::string, bool> response;

	opentelemetry::proto::trace::v1::TracesData trace_data;
	bool ret = trace_data.ParseFromString(raw_span_bucket_obj_content);
	if (false == ret) {
		std::cerr << "Error in calculate_trace_id_to_attribute_map:ParseFromString" << std::endl;
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

			if (indexed_attribute == curr_attr_key && attribute_value == curr_attr_val) {
				response[trace_id] = true;
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
bool is_batch_big_enough(index_batch& current_index_batch) {
	return (current_index_batch.total_trace_ids * 32) >= (1000000 - 200);
}

void export_batch_to_storage(index_batch& current_index_batch,
	std::string indexed_attribute, std::string attribute_value, gcs::Client* client
) {
	batch_timestamp consiledated_timestamp = batch_timestamp();
	std::string object_to_write = "";


	for (auto& elem : current_index_batch.trace_ids_with_timestamps) {
		auto curr_timestamp = elem.first;
		auto curr_trace_ids = elem.second;

		if (consiledated_timestamp.start_time == "" || 
			(std::stol(curr_timestamp.start_time) < std::stol(consiledated_timestamp.start_time))
		) {
			consiledated_timestamp.start_time = curr_timestamp.start_time;
		}

		if (consiledated_timestamp.end_time == "" || 
			(std::stol(curr_timestamp.end_time) > std::stol(consiledated_timestamp.end_time))
		) {
			consiledated_timestamp.end_time = curr_timestamp.end_time;
		}

		auto serialized_trace_ids = serialize_trace_ids(curr_trace_ids);
		auto serialized_timestamp = serialize_timestamp(curr_timestamp);

		object_to_write += (serialized_timestamp + "\n" + serialized_trace_ids);
	}

	std::string autoscaling_hash = get_autoscaling_hash_from_start_time(consiledated_timestamp.start_time);

	auto bucket_name = get_bucket_name_for_attr(indexed_attribute);
	auto folder_name = get_folder_name_from_attr_value(attribute_value);

	std::string object_name = folder_name + "/" + autoscaling_hash + "-" + \
		consiledated_timestamp.start_time + "-" + consiledated_timestamp.end_time;

	write_object(bucket_name, object_name, object_to_write, client);
	exit(1);
}

void write_object(std::string bucket_name, std::string object_name,
	std::string& object_to_write, gcs::Client* client) {

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

std::string serialize_timestamp(batch_timestamp timestamp) {
	std::string response = "start time: " + timestamp.start_time + ", " + "end time: " + timestamp.end_time;
	return response;
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

int dummy_tests() {
	// std::unordered_map<std::string, bool> global;
	// global["t1"] = false;
	// global["t2"] = true;
	// global["t5"] = true;

	// std::unordered_map<std::string, bool> local;
	// local["t1"] = true;
	// local["t2"] = false;
	// local["t3"] = true;
	// local["t4"] = false;

	// take_per_field_OR(global, local);

	// for (auto i : global) {
	// 	std::cout << i.first << " => " << i.second << std::endl;
	// }

	// std::size_t hash = std::hash<std::string>()("foo");	
	// std::cout << std::to_string(hash).substr(0, 2) << std::endl;
	// exit(1);
	return 0;
}