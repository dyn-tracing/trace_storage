// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#include "trace_attributes.h"

/**
 * There is no guarrantee that a bucket with this
 * name will exist. Thats up to you. 
 */
std::string get_bucket_name_for_attr(std::string indexed_attribute) {
	std::string bucket_name = indexed_attribute;
	replaceAll(bucket_name, ".", "-");
	return "index-" + bucket_name;
}

/**
 * TODO: Make sure that attr value can appear in the name of
 * gcs object.. maybe remove the special characters??
 */
std::string get_folder_name_from_attr_value(std::string attr_value) {
	return attr_value;
}

void replaceAll(std::string& str, const std::string& from, const std::string& to) {
	if(from.empty()) {
		return;
	}

	size_t start_pos = 0;
	while((start_pos = str.find(from, start_pos)) != std::string::npos) {
		str.replace(start_pos, from.length(), to);
		start_pos += to.length();  // In case 'to' contains 'from', like replacing 'x' with 'yx'
	}
}
