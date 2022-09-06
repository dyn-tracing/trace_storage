#include "common.h"

std::vector<std::string> split_by_string(const std::string& str, const char* ch) {
    std::vector<std::string> tokens;
    // https://stackoverflow.com/questions/14265581/parse-split-a-string-in-c-using-string-delimiter-standard-c

    auto start = 0;
    auto end = str.find(ch);
    while (end != std::string::npos) {
        tokens.push_back(strip_from_the_end(str.substr(start, end-start), '\n'));
        start = end + strlen(ch);
        end = str.find(ch, start);
    }
    tokens.push_back(strip_from_the_end(str.substr(start, end), '\n'));

    return tokens;
}

std::map<std::string, std::pair<int, int>> get_timestamp_map_for_trace_ids(
    const std::string &spans_data, const std::vector<std::string> &trace_ids) {
    std::map<std::string, std::pair<int, int>> response;

    ot::TracesData trace_data;
    bool ret = trace_data.ParseFromString(spans_data);
    if (false == ret) {
        std::cerr << "Error in ParseFromString" << std::endl;
        exit(1);
    }

    for (int i=0; i < trace_data.resource_spans(0).scope_spans(0).spans_size(); i++) {
        const ot::Span* sp = &trace_data.resource_spans(0).scope_spans(0).spans(i);
        std::string trace_id = hex_str(sp->trace_id(), sp->trace_id().length());

        // getting timestamps and converting from nanosecond precision to seconds precision
        int start_time = std::stoi(std::to_string(sp->start_time_unix_nano()).substr(0, 10));
        int end_time = std::stoi(std::to_string(sp->end_time_unix_nano()).substr(0, 10));

        response.insert(std::make_pair(trace_id, std::make_pair(start_time, end_time)));
    }

    return response;
}

std::string hex_str(const std::string &data, const int len) {
    std::string s(len * 2, ' ');
    for (int i = 0; i < len; ++i) {
        s[2 * i]     = hexmap[(data[i] & 0xF0) >> 4];
        s[2 * i + 1] = hexmap[data[i] & 0x0F];
    }

    return s;
}

bool is_same_hex_str(const std::string &data, const std::string &compare) {
    constexpr int len = 8;
    for (int i = 0; i < len; ++i) {
        if (compare[2 * i] != hexmap[(data[i] & 0xF0) >> 4]) {
            return false;
        }

        if (compare[2 * i + 1]  != hexmap[data[i] & 0x0F]) {
            return false;
        }
    }
    return true;
}



ot::TracesData read_object_and_parse_traces_data(
    const std::string &bucket, const std::string& object_name, gcs::Client* client
) {
    auto data = read_object(bucket, object_name, client);
    ot::TracesData trace_data;
    if (data == "") {
        return trace_data;
    }

    bool ret = trace_data.ParseFromString(data);
    if (!ret) {
        std::cerr << "Error in read_object_and_parse_traces_data:ParseFromString" << std::endl;
        exit(1);
    }

    return trace_data;
}

std::string read_object(const std::string &bucket, const std::string &object, gcs::Client* client) {
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

bool object_could_have_out_of_bound_traces(std::pair<int, int> batch_time, int start_time, int end_time) {
    std::pair<int, int> query_timespan = std::make_pair(start_time, end_time);

    // query timespan between object timespan
    if (batch_time.first < query_timespan.first && batch_time.second > query_timespan.second) {
        return true;
    }

    // batch timespan overlaps but starts before query timespan
    if (batch_time.first <= query_timespan.first && batch_time.second < query_timespan.second
    && batch_time.second >= query_timespan.first) {
        return true;
    }

    // vice versa
    if (batch_time.first > query_timespan.first && batch_time.second >= query_timespan.second
    && batch_time.first <= query_timespan.second) {
        return true;
    }

    return false;
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

std::string extract_batch_name(const std::string &object_name) {
    std::vector<std::string> result;
    boost::split(result, object_name, boost::is_any_of("/"));
    return result[1];
}

std::pair<int, int> extract_batch_timestamps(const std::string &batch_name) {
    std::vector<std::string> result;
    result.reserve(3);
    boost::split(result, batch_name, boost::is_any_of("-"));
    if (result.size() != 3) {
        std::cerr << "Error in extract_batch_timestamps with batch name: " << batch_name << std::endl;
        exit(1);
    }

    return std::make_pair(std::stoi(result[1]), std::stoi(result[2]));
}

std::vector<std::string> filter_trace_ids_based_on_query_timestamp(
    const std::vector<std::string> &trace_ids,
    const std::string &batch_name,
    const std::string &object_content,
    const int start_time,
    const int end_time,
    gcs::Client* client) {
    std::vector<std::string> response;

    std::map<std::string, std::string> trace_id_to_root_service_map = get_trace_id_to_root_service_map(object_content);
    std::map<std::string, std::vector<std::string>> root_service_to_trace_ids_map = get_root_service_to_trace_ids_map(
        trace_id_to_root_service_map);

    std::string buckets_suffix(BUCKETS_SUFFIX);
    for (auto const& elem : root_service_to_trace_ids_map) {
        std::map<std::string, std::pair<int, int>> trace_id_to_timestamp_map =
            get_timestamp_map_for_trace_ids(
                read_object(elem.first + buckets_suffix, batch_name, client),
                trace_ids);

        for (auto const& trace_id : elem.second) {
            std::pair<int, int> trace_timestamp = trace_id_to_timestamp_map[trace_id];
            if (is_object_within_timespan(trace_timestamp, start_time, end_time)) {
                response.push_back(trace_id);
            }
        }
    }

    return response;
}

std::map<std::string, std::string> get_trace_id_to_root_service_map(const std::string &object_content) {
    std::map<std::string, std::string> response;

    for (std::string i : split_by_string(object_content, "Trace ID: ")) {
        std::vector<std::string> trace = split_by_string(i, newline);
        std::string trace_id = trace[0].substr(0, TRACE_ID_LENGTH);
        for (int ind = 1; ind < trace.size(); ind ++) {
            if (trace[ind].substr(0, 1) == ":") {
                std::vector<std::string> root_span_info = split_by_string(trace[ind], colon);
                response.insert(std::make_pair(trace_id, root_span_info[2]));
                break;
            }
        }
    }

    return response;
}

std::map<std::string, std::vector<std::string>> get_root_service_to_trace_ids_map(
    const std::map<std::string, std::string> &trace_id_to_root_service_map) {
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

std::string extract_trace_from_traces_object(const std::string &trace_id, std::string& object_content) {
	const int start_ind = object_content.find("Trace ID: " + trace_id + ":");
	if (start_ind == std::string::npos) {
		return "";
	}

	int end_ind = object_content.find("Trace ID", start_ind+1);
	if (end_ind == std::string::npos) {
		// not necessarily required as end_ind=npos does the same thing, but for clarity:
		end_ind = object_content.length() - start_ind;
	}

	return strip_from_the_end(object_content.substr(start_ind, end_ind-start_ind), '\n');
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
void print_progress(float progress, std::string label, bool verbose) {
    if (!verbose) {
        return;
    }
    int barWidth = 70;
    std::cout << " [";
    int pos = barWidth * progress;
    for (int i = 0; i < barWidth; ++i) {
        if (i < pos) {
            std::cout << "=";
        } else if (i == pos) {
            std::cout << ">";
        } else {
            std::cout << " ";
        }
    }
    std::cout << "] " << int(progress * 100.0) << "% " << label << "\r";
    std::cout.flush();
}

std::string get_index_bucket_name(std::string property_name) {
    std::string bucket_name = property_name + BUCKETS_SUFFIX;
    replace_all(bucket_name, ".", "-");
    return bucket_name;
}