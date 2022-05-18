// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#include "trace_attributes.h"

/**
 * There is no guarrantee that a bucket with this
 * name will exist. Thats up to you. 
 */
std::string get_bucket_name_for_attr(std::string indexed_attribute) {
    if (indexed_attribute == ATTR_SPAN_KIND) {
        return "index-span-kind";
    }
    std::cerr << "Error in get_bucket_name_for_attr(" << indexed_attribute << ")" << std::endl;
    exit(1);
    return "";
}

/**
 * TODO: Make sure that attr value can appear in the name of
 * gcs object.. maybe remove the special characters??
 */
std::string get_folder_name_from_attr_value(std::string attr_value) {
    return attr_value;
}
