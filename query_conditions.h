// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#ifndef QUERY_CONDITIONS_H_
#define QUERY_CONDITIONS_H_

#include <string>
#include "opentelemetry/proto/trace/v1/trace.pb.h"

enum property_comparison {
	Equal_to,
	Lesser_than,
	Greater_than
};

enum node_properties {
	Latency,  // string(nano-secons)
	Start_time,  // string(nano-seconds)
	End_time,  // string(nano-seconds)
	Attribute_parent_name  // string
};

struct query_condition {
	int node_index;
    node_properties node_property_name;
	std::string node_property_value;
	property_comparison comp;
};

bool does_latency_condition_hold(opentelemetry::proto::trace::v1::Span sp, query_condition condition);
bool does_start_time_condition_hold(opentelemetry::proto::trace::v1::Span sp, query_condition condition);
bool does_end_time_condition_hold(opentelemetry::proto::trace::v1::Span sp, query_condition condition);

#endif  // QUERY_CONDITIONS_H_
