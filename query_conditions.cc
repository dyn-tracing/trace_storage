// Copyright 2022 Haseeb LLC
// @author: Muhammad Haseeb <mh6218@nyu.edu>

#include "query_conditions.h"

/**
 * TODO: There got to be a concise way to do all this stuff.. directly getting function name
 * from `condition` and invoking that using `reflection` maybe???
 * 
 */

bool does_latency_condition_hold(const opentelemetry::proto::trace::v1::Span* sp, query_condition condition) {
    auto latency = sp->end_time_unix_nano() - sp->start_time_unix_nano();

    switch (condition.comp) {
        case Equal_to:
            return latency == std::stol(condition.node_property_value);
        case Lesser_than:
            return latency < std::stol(condition.node_property_value);
        case Greater_than:
            return latency > std::stol(condition.node_property_value);
        default:
            std::cout << "damnn" << std::endl;
            return false;
    }

    return false;
}

bool does_start_time_condition_hold(const opentelemetry::proto::trace::v1::Span* sp, query_condition condition) {
    auto start_time = sp->start_time_unix_nano();

    switch (condition.comp) {
        case Equal_to:
            return start_time == std::stol(condition.node_property_value);
        case Lesser_than:
            return start_time < std::stol(condition.node_property_value);
        case Greater_than:
            return start_time > std::stol(condition.node_property_value);
        default:
            return false;
    }

    return false;
}

bool does_end_time_condition_hold(const opentelemetry::proto::trace::v1::Span* sp, query_condition condition) {
    auto end_time = sp->end_time_unix_nano();

    switch (condition.comp) {
        case Equal_to:
            return end_time == std::stol(condition.node_property_value);
        case Lesser_than:
            return end_time < std::stol(condition.node_property_value);
        case Greater_than:
            return end_time > std::stol(condition.node_property_value);
        default:
            return false;
    }

    return false;
}
