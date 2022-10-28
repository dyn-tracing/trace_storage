#ifndef QUERY_CONDITIONS_H_  // NOLINT
#define QUERY_CONDITIONS_H_  // NOLINT

#include <string>
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "common.h"

enum property_comparison {
	Equal_to,
	Less_than,
	Greater_than
};

// https://stackoverflow.com/questions/16770690/function-pointer-to-different-functions-with-different-arguments-in-c
typedef union {
  std::string (ot::Span::*string_func)() const;
  bool (ot::Span::*bool_func)() const;
  uint64_t (ot::Span::*int_func)() const;
  double (ot::Span::*double_func)() const;
  const std::string &(ot::Span::*bytes_func)() const; // NOLINT
} get_value_func;

enum property_type {
    string_value,
    bool_value,
    int_value,
    double_value,
    bytes_value
};

struct return_value {
    int node_index;
    property_type type;
    get_value_func func;
};

struct query_condition {
	int node_index;
    property_type type;
    get_value_func func;
	std::string node_property_value;
	property_comparison comp;
    std::string property_name;
    bool is_latency_condition;
};

std::string get_value_as_string(const ot::Span* sp,
    get_value_func val_func, property_type prop_type);
bool does_condition_hold(const ot::Span* sp, query_condition condition);

bool does_latency_condition_hold(const ot::Span* sp, query_condition condition);
bool does_start_time_condition_hold(const ot::Span* sp, query_condition condition);
bool does_end_time_condition_hold(const ot::Span* sp, query_condition condition);

#endif  // QUERY_CONDITIONS_H_ // NOLINT
