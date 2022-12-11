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
        try {
            int start_time = std::stoi(std::to_string(sp->start_time_unix_nano()).substr(0, 10));
            int end_time = std::stoi(std::to_string(sp->end_time_unix_nano()).substr(0, 10));

            response.insert(std::make_pair(trace_id, std::make_pair(start_time, end_time)));
        } catch (...) {
            std::cerr << "..." << std::endl;
        }
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
    auto data_ = read_object(bucket, object_name, client);
    if (!data_.ok()) {
        std::cout << "data is not okay because " << data_.status().message() << std::endl;
        exit(1);
    }
    auto data = data_.value();

    ot::TracesData trace_data;
    if (data == "") {
        return trace_data;
    }

    bool ret = trace_data.ParseFromString(data);
    if (!ret) {
        std::cerr << "Error in read_object_and_parse_traces_data:ParseFromString" << std::endl;
        std::cerr << "while reading object " << object_name << std::endl;
        exit(1);
    }

    return trace_data;
}

StatusOr<std::string> read_object(const std::string &bucket, const std::string &object, gcs::Client* client) {
    auto reader = client->ReadObject(bucket, object);
    if (!reader) {
        return reader.status();
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
                read_object(elem.first + buckets_suffix, batch_name, client).value(),
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
        for (uint64_t ind = 1; ind < trace.size(); ind ++) {
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
	for (const std::string & curr_trace_id : trace_ids) {
		const std::string res = extract_trace_from_traces_object(curr_trace_id, object_content);
		if (res != "") {
			return res;
		}
	}
	return "";
}

std::string extract_trace_from_traces_object(const std::string &trace_id, std::string& object_content) {
	const std::size_t start_ind = object_content.find("Trace ID: " + trace_id + ":");
	if (start_ind == std::string::npos) {
		return "";
	}

	std::size_t end_ind = object_content.find("Trace ID", start_ind+1);
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

bool has_suffix(std::string fullString, std::string ending) {
    if (fullString.length() >= ending.length()) {
        return (0 == fullString.compare (fullString.length() - ending.length(), ending.length(), ending));
    }
    return false;
}

std::vector<std::string> get_spans_buckets_names(gcs::Client* client) {
    std::vector<std::string> response;

    for (auto&& bucket_metadata : client->ListBucketsForProject(PROJECT_ID)) {
        if (!bucket_metadata) {
            std::cerr << bucket_metadata.status().message() << std::endl;
            exit(1);
        }

        if (false == has_suffix(bucket_metadata->name(), BUCKETS_SUFFIX)) {
            continue;
        }

        if (true == bucket_metadata->labels().empty()) {
            continue;
        }

        for (auto const& kv : bucket_metadata->labels()) {
            if (kv.first == BUCKET_TYPE_LABEL_KEY && kv.second == BUCKET_TYPE_LABEL_VALUE_FOR_SPAN_BUCKETS &&
                bucket_metadata->name().find(BUCKETS_SUFFIX) != std::string::npos) {
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

std::vector<std::string> generate_prefixes(time_t earliest, time_t latest) {
    // you want to generate a list of prefixes between earliest and latest
    // find the first digit at which they differ, then do a list on lowest to highest there
    // is this the absolute most efficient?  No, but at a certain point the network calls cost,
    // and I think this is good enough.
    std::vector<std::string> to_return;
    if (earliest == latest) {
        to_return.push_back(std::to_string(earliest));
        return to_return;
    }

    std::stringstream e;
    e << earliest;
    std::stringstream l;
    l << latest;

    std::string e_str = e.str();
    std::string l_str = l.str();

    int i = 0;
    for ( ; i < e_str.length(); i++) {
        if (e_str[i] != l_str[i]) {
            break;
        }
    }

    // i is now the first spot of difference

    int min = std::stoi(e_str.substr(i, 1));
    int max = std::stoi(l_str.substr(i, 1));


    for (int j = min; j <= max; j++) {
        std::string prefix = e_str.substr(0, i);
        prefix += std::to_string(j);
        to_return.push_back(prefix);
    }
    return to_return;
}

std::vector<std::string> get_list_result(gcs::Client* client, std::string prefix, time_t earliest, time_t latest) {
    std::vector<std::string> to_return;
    std::string trace_struct_bucket(TRACE_STRUCT_BUCKET_PREFIX);
    std::string suffix(BUCKETS_SUFFIX);
    for (auto&& object_metadata : client->ListObjects(trace_struct_bucket+suffix, gcs::Prefix(prefix))) {
        if (!object_metadata) {
            throw std::runtime_error(object_metadata.status().message());
        }
        // before we push back, should make sure that it's actually between the bounds
        std::string name = object_metadata->name();
        std::vector<std::string> times = split_by_string(name, hyphen);
        // we care about three of these:
        // if we are neatly between earliest and latest, or if we overlap on one side
        if (less_than(earliest, times[1]) && less_than(earliest, times[2])) {
            // we're too far back, already indexed this, ignore
            continue;
        } else if (greater_than(latest, times[1]) && greater_than(latest, times[2])) {
            // we're too far ahead;  we're still in the waiting period for this data
            continue;
        } else {
            to_return.push_back(name);
        }
    }
    return to_return;
}

std::vector<std::string> get_batches_between_timestamps(gcs::Client* client, time_t earliest, time_t latest) {
    std::vector<std::string> prefixes = generate_prefixes(earliest, latest);
    std::vector<std::future<std::vector<std::string>>> object_names;
    for (uint64_t i = 0; i < prefixes.size(); i++) {
        for (int j = 0; j < 10; j++) {
            for (int k=0; k < 10; k++) {
                std::string new_prefix = std::to_string(j) + std::to_string(k) + "-" + prefixes[i];
                object_names.push_back(
                    std::async(std::launch::async, get_list_result, client, new_prefix, earliest, latest));
            }
        }
    }
    std::vector<std::string> to_return;
    for (uint64_t m=0; m < object_names.size(); m++) {
        auto names = object_names[m].get();
        for (uint64_t n=0; n < names.size(); n++) {
            // check that these are actually within range
            std::vector<std::string> timestamps = split_by_string(names[n], hyphen);
            std::stringstream stream;
            stream << timestamps[1];
            std::string str = stream.str();
            time_t start_time = stol(str);

            std::stringstream end_stream;
            end_stream << timestamps[2];
            std::string end_str = end_stream.str();
            time_t end_time = stol(end_str);

            if ((start_time >= earliest && end_time <= latest) ||
                (start_time <= earliest && end_time >= earliest) ||
                (start_time <= latest && end_time >= latest)
            ) {
                to_return.push_back(names[n]);
            }
        }
    }
    return to_return;
}

bool less_than(time_t first, std::string second) {
    std::stringstream sec_stream;
    sec_stream << second;
    std::string sec_str = sec_stream.str();
    time_t s = stol(sec_str);
    return first < s;
}

bool greater_than(time_t first, std::string second) {
    std::stringstream sec_stream;
    sec_stream << second;
    std::string sec_str = sec_stream.str();
    time_t s = stol(sec_str);
    return first > s;
}

time_t time_t_from_string(std::string str) {
    std::stringstream stream;
    stream << str;
    std::string sec_str = stream.str();
    return stol(sec_str);
}

void merge_objname_to_trace_ids(objname_to_matching_trace_ids &original,
                                objname_to_matching_trace_ids &to_empty) {
    for (auto && map : to_empty) {
        std::string batch_name = map.first;
        std::vector<std::string> trace_ids = map.second;
        if (original.find(batch_name) == original.end()) {
            original[batch_name] = trace_ids;
        } else {
            original[batch_name].insert(original[batch_name].end(),
                                        trace_ids.begin(), trace_ids.end());
        }
    }
}

time_t get_lowest_time_val(gcs::Client* client) {
    std::string trace_struct_bucket(TRACE_STRUCT_BUCKET_PREFIX);
    std::string suffix(BUCKETS_SUFFIX);
    std::string bucket_name = trace_struct_bucket+suffix;
    time_t now;
    time(&now);
    time_t lowest_val = now;
    for (int i=0; i < 10; i++) {
        for (int j=0; j < 10; j++) {
            std::string prefix = std::to_string(i) + std::to_string(j);
            for (auto&& object_metadata :
                client->ListObjects(bucket_name, gcs::Prefix(prefix))) {
                if (!object_metadata) {
                    throw std::runtime_error(object_metadata.status().message());
                }
                std::string object_name = object_metadata->name();
                auto split = split_by_string(object_name, hyphen);
                time_t low = time_t_from_string(split[1]);
                if (low < lowest_val) {
                    lowest_val = low;
                }
                // we break because we don't want to read all values, just first one
                break;
            }
        }
    }
    return lowest_val;
}
