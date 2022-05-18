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

enum property_type {
    string_value,
    bool_value,
    int_value,
    double_value,
    bytes_value
};


//https://stackoverflow.com/questions/16770690/function-pointer-to-different-functions-with-different-arguments-in-c
typedef union {
  std::string (opentelemetry::proto::trace::v1::Span::*string_func)() const;
  bool (opentelemetry::proto::trace::v1::Span::*bool_func)() const;
  uint64_t (opentelemetry::proto::trace::v1::Span::*int_func)() const;
  double (opentelemetry::proto::trace::v1::Span::*double_func)() const;
  char* (opentelemetry::proto::trace::v1::Span::*bytes_func)() const;
} get_value_func;

struct query_condition {
	int node_index;
    property_type type;
    get_value_func func;
	std::string node_property_value;
	property_comparison comp;
};

bool does_condition_hold(const opentelemetry::proto::trace::v1::Span* sp, query_condition condition);

bool does_latency_condition_hold(const opentelemetry::proto::trace::v1::Span* sp, query_condition condition);
bool does_start_time_condition_hold(const opentelemetry::proto::trace::v1::Span* sp, query_condition condition);
bool does_end_time_condition_hold(const opentelemetry::proto::trace::v1::Span* sp, query_condition condition);

#endif  // QUERY_CONDITIONS_H_
