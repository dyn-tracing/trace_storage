// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <stdexcept>
#include <regex>
#include <future>
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>

const char trace_struct_bucket[] = "dyntraces-snicket4";
const char ending[] = "-snicket4";
// Create aliases to make the code easier to read.
namespace gcs = ::google::cloud::storage;

std::vector<std::string> split_string_by_newline(const std::string& str) {
	std::vector<std::string> tokens;
	split_regex(tokens, str, boost::regex("(\n)+"));
	return tokens;
}

std::vector<std::string> split_string_by_colon(const std::string& str) {
	std::vector<std::string> tokens;
	split_regex(tokens, str, boost::regex("(:)+"));
	return tokens;
}

// https://codereview.stackexchange.com/questions/78535/converting-array-of-bytes-to-the-hex-string-representation
constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7',
						   '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

std::string hex_str(std::string data, int len) {
	std::string s(len * 2, ' ');
	for (int i = 0; i < len; ++i) {
		s[2 * i]     = hexmap[(data[i] & 0xF0) >> 4];
		s[2 * i + 1] = hexmap[data[i] & 0x0F];
	}
	return s;
}

opentelemetry::proto::trace::v1::Span get_span(
    int hash1, int hash2, std::string microservice, std::string start_time,
    std::string end_time, gcs::Client* client, std::string span_id) {
    std::string obj_name = std::to_string(hash1) + std::to_string(hash2);
    obj_name = obj_name + "-" + start_time + "-" + end_time;
    auto reader = client->ReadObject(microservice+ending, obj_name);
    if (reader.status().code() == ::google::cloud::StatusCode::kNotFound) {
        std::cerr << "span object not found " << obj_name <<
            "in microservice " << microservice << std::endl;
        throw std::runtime_error("span object not found");
    } else if (!reader) {
        std::cerr << "Error reading object: " << reader.status() << "\n";
        throw std::runtime_error("Error reading trace object");
    } else {
        std::string contents{std::istreambuf_iterator<char>{reader}, {}};
        opentelemetry::proto::trace::v1::TracesData trace_data;
        bool ret = trace_data.ParseFromString(contents);
        if (!ret) {
            throw std::runtime_error("could not parse span data");
        }
        int sp_size = trace_data.resource_spans(0).scope_spans(0).spans_size();
        for (int i=0; i < sp_size; i++) {
            opentelemetry::proto::trace::v1::Span sp =
                trace_data.resource_spans(0).scope_spans(0).spans(i);
            if (hex_str(sp.span_id(), sp.span_id().length()) == span_id) {
                return sp;
            }
        }
    }
    std::cerr << "did not find span object " << span_id << std::endl;
    throw std::runtime_error("did not find span object");
}

// Gets a trace by trace ID and given timespan
std::vector<std::future<opentelemetry::proto::trace::v1::Span>> get_trace(
    std::string traceID, int start_time, int end_time, gcs::Client* client) {
    std::vector<std::future<opentelemetry::proto::trace::v1::Span>> to_return;
    bool trace_found = false;
    for (int i=0; i < 10; i++) {
        if (trace_found) {
            break;
        }
        for (int j=0; j < 10; j++) {
          if (trace_found) {
            break;
          }
          std::string obj_name = std::to_string(i) + std::to_string(j) + "-";
          obj_name += std::to_string(start_time) + "-" + std::to_string(end_time);
          auto reader = client->ReadObject(trace_struct_bucket, obj_name);
          if (reader.status().code() ==
            ::google::cloud::StatusCode::kNotFound) {
            continue;
          } else if (!reader) {
            std::cerr << "Error reading trace object: " << reader.status()
                << "\n";
            throw std::runtime_error("error reading trace object");
          } else {
            std::string contents{std::istreambuf_iterator<char>{reader}, {}};
            int traceID_location = contents.find(traceID);
            if (traceID_location) {
                trace_found = true;
                int end = contents.find("Trace ID", traceID_location-1);
                // TODO(jessica):  will searching for this work if we have the last trace in the file?
                if (end) {
                    std::string spans = contents.substr(
                        traceID_location, end-traceID_location);
                    std::vector<std::string> split_spans;
                    split_spans = split_string_by_newline(spans);
                    // start at 1 because first line will be trace ID
                    for (int k = 1; k < split_spans.size(); k++) {
                        if (split_spans[k] != "") {
                            std::vector<std::string> span_info;
                            span_info = split_string_by_colon(split_spans[k]);
                            std::future<opentelemetry::proto::trace::v1::Span> sp;
                            to_return.push_back(std::async(std::launch::async, get_span,
                                i, j, span_info[2],
                                std::to_string(start_time),
                                std::to_string(end_time), client, span_info[1]));
                        }
                    }


                } else { throw std::runtime_error("couldn't find trace ID"); }
            }
          }
        }
    }
    return to_return;
}

int main(int argc, char* argv[]) {
    // Create a client to communicate with Google Cloud Storage. This client
    // uses the default configuration for authentication and project id.
    auto client = gcs::Client();
    auto span_futures = get_trace("0000898de3ac90dc60a138fbc9c9d6b0",
                     1651500643, 1651500644, &client);
    std::cout << "len span_futures: " << span_futures.size() << std::endl;
    for (int i=0; i < span_futures.size(); i++) {
        auto span = span_futures[i].get();
        std::cout << "span id " << hex_str(span.span_id(), span.span_id().length())
        << " " << span.name() << " " << std::to_string(span.start_time_unix_nano()) << std::endl;
    }
    return 0;
}
