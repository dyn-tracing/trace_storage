#include "query_conditions.h"

std::string get_value_as_string(const ot::Span* sp,
    const get_value_func val_func, const property_type prop_type) {
    switch (prop_type) {
        case string_value:
            return (sp->*val_func.string_func)();
        case bool_value: {
            bool val = (sp->*val_func.bool_func)();
            std::string span_val;
            if (val) {
                span_val = "false";
            } else {
                span_val = "true";
            }
            return span_val;
        }
        case int_value: {
            int val = ((sp->*val_func.int_func)());
            return std::to_string(val);
        }
        case double_value: {
            double val = ((sp->*val_func.double_func)());
            return std::to_string(val);
        }
        case bytes_value: {
            std::string bytes_str = ((sp->*val_func.bytes_func)());
            return hex_str(bytes_str, bytes_str.size());
        }
        default:
            return "";
    }
}


bool does_condition_hold(const ot::Span* sp, const query_condition condition) {
    switch (condition.type) {
        case string_value: {
            const std::string span_value = (sp->*condition.func.string_func)();
            switch (condition.comp) {
                case Equal_to:
                    return span_value.compare(condition.node_property_value) == 0;
                default:
                    return span_value.compare(condition.node_property_value);
            }
        }
        case bool_value: {
            const bool val = (sp->*condition.func.bool_func)();
            std::string span_val;
            if (val) {
                span_val = "false";
            } else {
                span_val = "true";
            }
            switch (condition.comp) {
                case Equal_to:
                    return span_val.compare(condition.node_property_value) == 0;
                default:
                    return false;  // it is undefined to be "less than" or "greater than" a bool
            }
        }
        case int_value: {
            const int span_val = (sp->*condition.func.int_func)();
            const int cond_val = std::stoi(condition.node_property_value);
            switch (condition.comp) {
                case Equal_to: return span_val == cond_val;
                case Lesser_than: return span_val < cond_val;
                case Greater_than: return span_val > cond_val;
            }
        }
        case double_value: {
            const double span_val = (sp->*condition.func.double_func)();
            const double cond_val = std::stod(condition.node_property_value);
            switch (condition.comp) {
                case Equal_to: return span_val == cond_val;
                case Lesser_than: return span_val < cond_val;
                case Greater_than: return span_val > cond_val;
            }
        }
        case bytes_value: {
            // for bytes, we use a string representation
            const std::string span_value = (sp->*condition.func.bytes_func)();
            switch (condition.comp) {
                case Equal_to:
                    return hex_str(span_value, span_value.size()).compare(condition.node_property_value) == 0;
                default:
                    return hex_str(span_value, span_value.size()).compare(condition.node_property_value);
            }
        }
    }
}

/**
 * TODO: There got to be a concise way to do all this stuff.. directly getting function name
 * from `condition` and invoking that using `reflection` maybe???
 * 
 */

bool does_latency_condition_hold(const ot::Span* sp, const query_condition condition) {
    const auto latency = sp->end_time_unix_nano() - sp->start_time_unix_nano();

    switch (condition.comp) {
        case Equal_to:
            return latency == std::stoul(condition.node_property_value);
        case Lesser_than:
            return latency < std::stoul(condition.node_property_value);
        case Greater_than:
            return latency > std::stoul(condition.node_property_value);
        default:
            return false;
    }

    return false;
}

bool does_start_time_condition_hold(const ot::Span* sp, const query_condition condition) {
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

bool does_end_time_condition_hold(const ot::Span* sp, const query_condition condition) {
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
