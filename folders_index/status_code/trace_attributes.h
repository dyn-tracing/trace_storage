// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#ifndef TRACE_ATTRIBUTES_H_  // NOLINT
#define TRACE_ATTRIBUTES_H_  // NOLINT

#include <iostream>
#include <string>

const char ATTR_SPAN_KIND[] = "span.kind";
const char ATTR_NET_PEER_IP[] = "net.peer.ip";

std::string get_bucket_name_for_attr(std::string indexed_attribute);
std::string get_folder_name_from_attr_value(std::string attr_value);

#endif  // TRACE_ATTRIBUTES_H_  // NOLINT