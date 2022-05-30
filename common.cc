#include "common.h"

std::vector<std::string> split_by_string(std::string& str, const char* ch) {
    std::vector<std::string> tokens;
    std::string ch_str(ch);
    std::string reg = "(" + ch_str + ")+";
    split_regex(tokens, str, boost::regex(reg));

    std::vector<std::string> response;
    for (int i = 0; i < tokens.size(); i++) {
        response.push_back(strip_from_the_end(tokens[i], '\n'));
    }
    return response;
}

std::map<std::string, std::pair<int, int>> get_timestamp_map_for_trace_ids(
    std::string spans_data, std::vector<std::string> trace_ids) {
    std::map<std::string, std::pair<int, int>> response;

    opentelemetry::proto::trace::v1::TracesData trace_data;
    bool ret = trace_data.ParseFromString(spans_data);
    if (false == ret) {
        std::cerr << "Error in ParseFromString" << std::endl;
        exit(1);
    }

    for (int i=0; i < trace_data.resource_spans(0).scope_spans(0).spans_size(); i++) {
        opentelemetry::proto::trace::v1::Span sp = trace_data.resource_spans(0).scope_spans(0).spans(i);

        std::string trace_id = hex_str(sp.trace_id(), sp.trace_id().length());

        // getting timestamps and converting from nanosecond precision to seconds precision
        int start_time = std::stoi(std::to_string(sp.start_time_unix_nano()).substr(0, 10));
        int end_time = std::stoi(std::to_string(sp.end_time_unix_nano()).substr(0, 10));

        response.insert(std::make_pair(trace_id, std::make_pair(start_time, end_time)));
    }

    return response;
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



opentelemetry::proto::trace::v1::TracesData read_object_and_parse_traces_data(
    std::string bucket, std::string object_name, gcs::Client* client
) {
    auto data = read_object(bucket, object_name, client);
    opentelemetry::proto::trace::v1::TracesData trace_data;
    if (data == "") {
        return trace_data;
    }

    bool ret = trace_data.ParseFromString(data);
    if (false == ret) {
        std::cerr << "Error in read_object_and_parse_traces_data:ParseFromString" << std::endl;
        exit(1);
    }

    return trace_data;
}

std::string read_object(std::string bucket, std::string object, gcs::Client* client) {
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

bool is_object_within_timespan(std::pair<int, int> batch_time, int start_time, int end_time) {
    std::pair<int, int> query_timespan = std::make_pair(start_time, end_time);

    // query timespan between object timespan
    if (batch_time.first <= query_timespan.first && batch_time.second >= query_timespan.second) {
        return true;
    }

    // query timespan contains object timespan
    if (batch_time.first >= query_timespan.first && batch_time.second <= query_timespan.second) {
        return true;
    }

    // batch timespan overlaps but starts before query timespan
    if (batch_time.first <= query_timespan.first && batch_time.second <= query_timespan.second
    && batch_time.second >= query_timespan.first) {
        return true;
    }

    // vice versa
    if (batch_time.first >= query_timespan.first && batch_time.second >= query_timespan.second
    && batch_time.first <= query_timespan.second) {
        return true;
    }

    return false;
}

std::string strip_from_the_end(std::string object, char stripper) {
    if (!object.empty() && object[object.length()-1] == stripper) {
        object.erase(object.length()-1);
    }
    return object;
}

std::string extract_batch_name(std::string object_name) {
    std::vector<std::string> result;
    boost::split(result, object_name, boost::is_any_of("/"));

    return result[1];
}

std::pair<int, int> extract_batch_timestamps(std::string batch_name) {
    std::vector<std::string> result;
    boost::split(result, batch_name, boost::is_any_of("-"));
    if (result.size() != 3) {
        std::cerr << "Error in extract_batch_timestamps with batch name: " << batch_name << std::endl;
        exit(1);
    }

    return std::make_pair(std::stoi(result[1]), std::stoi(result[2]));
}

std::vector<std::string> filter_trace_ids_based_on_query_timestamp(
    std::vector<std::string> trace_ids,
    std::string batch_name,
    std::string object_content,
    int start_time,
    int end_time,
    gcs::Client* client) {
    std::vector<std::string> response;

    std::map<std::string, std::string> trace_id_to_root_service_map = get_trace_id_to_root_service_map(object_content);
    std::map<std::string, std::vector<std::string>> root_service_to_trace_ids_map = get_root_service_to_trace_ids_map(
        trace_id_to_root_service_map);

    std::string buckets_suffix(BUCKETS_SUFFIX);
    for (auto const& elem : root_service_to_trace_ids_map) {
        std::string bucket = elem.first + buckets_suffix;
        std::string spans_data = read_object(bucket, batch_name, client);

        std::map<std::string, std::pair<int, int>>
        trace_id_to_timestamp_map = get_timestamp_map_for_trace_ids(spans_data, trace_ids);

        std::vector<std::string> successful_trace_ids;
        for (auto const& trace_id : elem.second) {
            std::pair<int, int> trace_timestamp = trace_id_to_timestamp_map[trace_id];
            if (is_object_within_timespan(trace_timestamp, start_time, end_time)) {
                successful_trace_ids.push_back(trace_id);
            }
        }

        response.insert(response.end(), successful_trace_ids.begin(), successful_trace_ids.end());
    }

    return response;
}
std::map<std::string, std::string> get_trace_id_to_root_service_map(std::string object_content) {
    std::map<std::string, std::string> response;
    std::vector<std::string> all_traces = split_by_string(object_content, "Trace ID: ");

    for (std::string i : all_traces) {
        std::vector<std::string> trace = split_by_string(i, newline);
        std::string trace_id = trace[0].substr(0, TRACE_ID_LENGTH);
        for (int ind = 1; ind < trace.size(); ind ++) {
            if (trace[ind].substr(0, 1) == ":") {
                std::vector<std::string> root_span_info = split_by_string(trace[ind], colon);
                std::string root_service = root_span_info[2];
                response.insert(std::make_pair(trace_id, root_service));
                break;
            }
        }
    }

    return response;
}

std::map<std::string, std::vector<std::string>> get_root_service_to_trace_ids_map(
    std::map<std::string, std::string> trace_id_to_root_service_map) {
    std::map<std::string, std::vector<std::string>> response;

    for (auto const& elem : trace_id_to_root_service_map) {
        response[elem.second].push_back(elem.first);
    }

    return response;
}

std::string extract_any_trace(std::vector<std::string>& trace_ids, std::string& object_content) {
	for (auto& curr_trace_id : trace_ids) {
		auto res = extract_trace_from_traces_object(curr_trace_id, object_content);
		if (res != "") {
			return res;
		}
	}

	return "";
}

std::string extract_trace_from_traces_object(std::string trace_id, std::string& object_content) {
	int start_ind = object_content.find("Trace ID: " + trace_id + ":");
	if (start_ind == std::string::npos) {
		// std::cerr << "trace_id (" << trace_id << ") not found in the object_content" << std::endl;
		return "";
	}

	int end_ind = object_content.find("Trace ID", start_ind+1);
	if (end_ind == std::string::npos) {
		// not necessarily required as end_ind=npos does the same thing, but for clarity:
		end_ind = object_content.length() - start_ind;
	}

	std::string trace = object_content.substr(start_ind, end_ind-start_ind);
	trace = strip_from_the_end(trace, '\n');
	return trace;
}

void replace_all(std::string& str, const std::string& from, const std::string& to) {
    if (from.empty()) {
        return;
    }

    size_t start_pos = 0;
    while ((start_pos = str.find(from, start_pos)) != std::string::npos) {
        str.replace(start_pos, from.length(), to);
        start_pos += to.length();  // In case 'to' contains 'from', like replacing 'x' with 'yx'
    }
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

        for (auto const& kv : bucket_metadata->labels()) {
            if (kv.first == BUCKET_TYPE_LABEL_KEY && kv.second == BUCKET_TYPE_LABEL_VALUE_FOR_SPAN_BUCKETS) {
                response.push_back(bucket_metadata->name());
            }
        }
    }

    return response;
}

// https://stackoverflow.com/questions/14539867/how-to-display-a-progress-indicator-in-pure-c-c-cout-printf
void print_progress(float progress, std::string label) {
    int barWidth = 70;
    std::cout << " [";
    int pos = barWidth * progress;
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos) std::cout << "=";
        else if (i == pos) std::cout << ">";
        else std::cout << " ";
    }
    std::cout << "] " << int(progress * 100.0) << "% " << label << "\r";
    std::cout.flush();
}
