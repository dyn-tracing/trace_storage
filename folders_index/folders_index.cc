// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#include "folders_index.h"

int update_index(gcs::Client* client, time_t last_updated, std::string indexed_attribute
) {
	std::vector<std::string> span_buckets_names = get_spans_buckets_names(client);
	std::vector<std::string> trace_struct_object_names = get_all_object_names(TRACE_STRUCT_BUCKET, client);
	trace_struct_object_names = sort_object_names_on_start_time(trace_struct_object_names);

	int thread_pool_size = 500;
	for (int i=0; i < trace_struct_object_names.size(); i=i+thread_pool_size) {
		update_index_batched(client, last_updated, indexed_attribute, span_buckets_names,
			trace_struct_object_names, i, thread_pool_size);
	}

	return 0;
}

void update_index_batched(gcs::Client* client, time_t last_updated, std::string indexed_attribute,
	std::vector<std::string> span_buckets_names, std::vector<std::string>& trace_struct_object_names,
	int batch_start_ind, int batch_size
) {
	std::vector<
		std::pair<
			std::string,
			std::future<
				std::unordered_map<
					std::string,
					std::vector<
						std::string>>>>> response_futures;

	for (int i = batch_start_ind; (i < trace_struct_object_names.size() && i < batch_start_ind+batch_size); i++) {
		auto object_name = trace_struct_object_names[i];
		if(is_batch_older_than_last_updated(object_name, last_updated)) {
			continue;
		}

		response_futures.push_back(std::make_pair(object_name, std::async(std::launch::async,
			get_attr_to_trace_ids_map, object_name, indexed_attribute,
			std::ref(span_buckets_names), client)));
	}

	index_batch current_index_batch = index_batch();
	for (int i = 0; i < response_futures.size(); i++) {
		auto object_name = response_futures[i].first;
		auto attr_to_trace_ids_map = response_futures[i].second.get();

		current_index_batch.trace_ids_with_timestamps.push_back(std::make_pair(object_name, attr_to_trace_ids_map));

		auto to_export = get_attr_vals_which_have_enough_data_to_export(current_index_batch);

		if (to_export.size() > 0) {
			export_batch_to_storage(current_index_batch, indexed_attribute, to_export, client);
		}
	}

	export_batch_to_storage(current_index_batch, indexed_attribute, get_all_attr_values(current_index_batch), client);
	return;
}

int get_total_of_trace_ids(std::unordered_map<std::string, std::vector<std::string>> attr_to_trace_ids) {
	int count = 0;
	for (auto& ele : attr_to_trace_ids) {
		count += ele.second.size();
	}
	return count;
}

/**
 * @brief Get the all attr values from index batch (e.g. for http errors, values are 404, 500 ...)
 */
std::vector<std::string> get_all_attr_values(index_batch& current_index_batch) {
	std::vector<std::string> response;
	for (auto& pair_of_time_and_map : current_index_batch.trace_ids_with_timestamps) {
		for (auto& map_ele : pair_of_time_and_map.second) {
			if (std::find(response.begin(), response.end(), map_ele.first) == response.end()) {
				response.push_back(map_ele.first);
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
	std::string object_name, std::string indexed_attribute,
	std::vector<std::string>& span_buckets_names, gcs::Client* client
) {
	std::unordered_map<std::string, std::vector<std::string>> attr_to_trace_ids_map;

	for (auto span_bucket : span_buckets_names) {
		std::unordered_map<
			std::string,
			std::vector<std::string>> local_attr_to_trace_ids_map = calculate_attr_to_trace_ids_map_for_microservice(
				span_bucket, object_name, indexed_attribute, client);

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
					(*local_trace_ids)[trace_id_ind]) == previous_trace_ids->end()
			) {
				attr_to_trace_ids_map[local_attribute].push_back((*local_trace_ids)[trace_id_ind]);
			}
		}
	}
}

std::unordered_map<std::string, std::vector<std::string>> calculate_attr_to_trace_ids_map_for_microservice(
	std::string span_bucket_name, std::string object_name, std::string indexed_attribute, gcs::Client* client
) {
	std::unordered_map<std::string, std::vector<std::string>> response;  // attr_val_to_vec_of_traceids
	std::string raw_span_bucket_obj_content = read_object2(span_bucket_name, object_name, client);
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
			std::string curr_attr_val = "";

			switch (val->value_case()) {
			case 1:
				curr_attr_val = val->string_value();
				break;
			case 2:
				curr_attr_val = val->bool_value() ? "true" : "false";
				break;
			case 3:
				curr_attr_val = std::to_string(val->int_value());
				break;
			case 4:
				curr_attr_val = std::to_string(val->double_value());
				break;
			default:
				std::cerr << "Not supported attr type." << std::endl;
				exit(1);
				break;
			}

			if (indexed_attribute == curr_attr_key) {
				response[curr_attr_val].push_back(trace_id); // NOLINT
			}
		}
	}

	return response;
}

batch_timestamp extract_batch_timestamps_struct(std::string batch_name) {
	std::vector<std::string> result;
	boost::split(result, batch_name, boost::is_any_of("-"));
	if (result.size() != 3) {
		std::cerr << "Error in extract_batch_timestamps_struct with batch name: " << batch_name << std::endl;
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
		if (map_ele.second*32 >= (1000000 - 100)) {  // Using TRACE_ID_LENGTH
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
 * @brief Exports index_batch to cloud storage. The batch at this point
 * might contain data that is not necessarily required to be imported 
 * right away so only the attributes specified in attrs_to_export are exported.
 * 
 * @param current_index_batch 
 * @param indexed_attribute 
 * @param attrs_to_export 
 * @param client 
 */
void export_batch_to_storage(index_batch& current_index_batch, std::string indexed_attribute,
	std::vector<std::string> attrs_to_export, gcs::Client* client
) {
	for (auto attr_being_exported : attrs_to_export) {
		batch_timestamp consiledated_timestamp = batch_timestamp();
		std::string object_to_write = "";

		for (auto& elem : current_index_batch.trace_ids_with_timestamps) {
			auto* attr_to_trace_ids_map = &(elem.second);

			if (attr_to_trace_ids_map->find(attr_being_exported) == attr_to_trace_ids_map->end()
			|| ((*attr_to_trace_ids_map)[attr_being_exported].size() < 1)) {
				continue;
			}

			auto curr_timestamp = extract_batch_timestamps_struct(elem.first);

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

			auto serialized_trace_ids = serialize_trace_ids((*attr_to_trace_ids_map)[attr_being_exported]);
			auto serialized_timestamp = "Timestamp: " + elem.first;

			object_to_write += (serialized_timestamp + "\n" + serialized_trace_ids);
		}

		if (object_to_write == "") {
			continue;
		}

		auto bucket_name = get_bucket_name_for_attr(indexed_attribute);
		auto folder_name = get_folder_name_from_attr_value(attr_being_exported);
		std::string autoscaling_hash = get_autoscaling_hash_from_start_time(consiledated_timestamp.start_time);
		std::string object_name = folder_name + "/" + autoscaling_hash + "-" + \
			consiledated_timestamp.start_time + "-" + consiledated_timestamp.end_time;

		write_object(bucket_name, object_name, object_to_write, client);
		update_last_updated_label_if_needed(bucket_name, consiledated_timestamp.end_time, client);
		remove_exported_data_from_index_batch(current_index_batch, attr_being_exported);
	}

	return;
}

void update_last_updated_label_if_needed(
	std::string bucket_name, std::string new_last_updated, gcs::Client* client
) {
	auto prev_last_updated = get_last_updated_for_bucket(bucket_name, client);
	time_t new_last_updated_t = (time_t) std::stol(new_last_updated, NULL, 10);
	if (prev_last_updated < new_last_updated_t) {
		update_bucket_label(bucket_name, "last_updated", new_last_updated, client);
	}
	return;
}

void update_bucket_label(std::string bucket_name, std::string label_key, std::string label_val, gcs::Client* client) {
	auto updated_metadata = client->PatchBucket(bucket_name, gcs::BucketMetadataPatchBuilder().SetLabel(
		label_key, label_val));

	if (!updated_metadata) {
		std::cerr << "Error in update_bucket_label " << updated_metadata.status().message() << std::endl;
		exit(1);
	}

	return;
}

void remove_exported_data_from_index_batch(index_batch& current_index_batch, std::string attr_to_remove) {
	int size = current_index_batch.trace_ids_with_timestamps.size();
	for (int i = size-1; i >= 0; i--) {
		auto* timestamp_and_data_pair = &(current_index_batch.trace_ids_with_timestamps[i]);
		auto* attr_to_traceids_map = &(timestamp_and_data_pair->second);
		auto* trace_ids = &((*attr_to_traceids_map)[attr_to_remove]);
		trace_ids->clear();
		attr_to_traceids_map->erase(attr_to_remove);
		// if (current_index_batch.trace_ids_with_timestamps[i].second.size() < 1) {
		// 	current_index_batch.trace_ids_with_timestamps.erase(
		// 			current_index_batch.trace_ids_with_timestamps.begin()+i);
		// }
	}
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
		std::cerr << "Error in write_object:" << object_name << " => " << metadata.status().message() << std::endl;
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

std::string read_object2(std::string bucket, std::string object, gcs::Client* client) {
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

std::string read_bucket_label(std::string bucket_name, std::string label_key, gcs::Client* client) {
	auto bucket_metadata = client->GetBucketMetadata(bucket_name);
	if (!bucket_metadata) {
		std::cerr << "Error in read_bucket_label: " << bucket_metadata.status().message() << std::endl;
	}

	for (auto const& kv : bucket_metadata->labels()) {
		if (kv.first == label_key) {
			return kv.second;
		}
	}

	return "";
}

time_t get_last_updated_for_bucket(std::string bucket_name, gcs::Client* client) {
	auto last_updated = read_bucket_label(bucket_name, "last_updated", client);
	if (last_updated == "") {
		return 0;
	}

	return (time_t) std::stol(last_updated, NULL, 10);
}

/**
 * TODO: should it be <= or < ???
 * seems like <=
 */
bool is_batch_older_than_last_updated(std::string batch_name, time_t last_updated) {
	auto timestamp = extract_batch_timestamps_struct(batch_name);
	return (time_t)std::stol(timestamp.end_time, NULL, 10) <= last_updated;
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
