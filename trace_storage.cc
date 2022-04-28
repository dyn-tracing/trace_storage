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

#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include <iostream>
#include <regex>
#include <boost/regex.hpp>
#include <boost/algorithm/string/regex.hpp>



const std::string trace_struct_bucket = "dyntraces-snicket3";
const std::string ending = "-snicket3";
// Create aliases to make the code easier to read.
namespace gcs = ::google::cloud::storage;

std::vector<std::string> split_string_by_newline(const std::string& str)
{
    std::vector<std::string> tokens;
    split_regex(tokens, str, boost::regex("(\n)+"));
    return tokens;
}

std::vector<std::string> split_string_by_colon(const std::string& str)
{
    std::vector<std::string> tokens;
    split_regex(tokens, str, boost::regex("(:)+"));
    return tokens;
}

// https://codereview.stackexchange.com/questions/78535/converting-array-of-bytes-to-the-hex-string-representation
constexpr char hexmap[] = {'0', '1', '2', '3', '4', '5', '6', '7',
                           '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

std::string hex_str(std::string data, int len)
{
  std::string s(len * 2, ' ');
  for (int i = 0; i < len; ++i) {
    s[2 * i]     = hexmap[(data[i] & 0xF0) >> 4];
    s[2 * i + 1] = hexmap[data[i] & 0x0F];
  }
  return s;
}


opentelemetry::proto::trace::v1::Span get_span(int hash1, int hash2, std::string microservice, std::string start_time, std::string end_time, gcs::Client* client, std::string span_id) {
    std::string obj_name = std::to_string(hash1) + std::to_string(hash2) + "-" + start_time + "-" + end_time;
    auto reader = client->ReadObject(microservice+ending, obj_name);
    if (reader.status().code() == ::google::cloud::StatusCode::kNotFound) {
        std::cerr << "span object not found " << obj_name << "in microservice " << microservice << std::endl;
    } else if (!reader) {
        std::cerr << "Error reading object: " << reader.status() << "\n";
    } else {
        std::string contents{std::istreambuf_iterator<char>{reader}, {}};
        opentelemetry::proto::trace::v1::TracesData trace_data;
        bool ret = trace_data.ParseFromString(contents);
        if (!ret) {
            std::cerr << " not parsed! " << std::endl;
        }
        for (int i=0; i<trace_data.resource_spans(0).scope_spans(0).spans_size(); i++) {
            opentelemetry::proto::trace::v1::Span sp = trace_data.resource_spans(0).scope_spans(0).spans(i);
            if (hex_str(sp.span_id(), sp.span_id().length()) == span_id) {
                return sp;

            }
        }
    }
    std::cerr << "did not find span object " << span_id << std::endl;
}

// Gets a trace by trace ID and given timespan
int get_trace(std::string traceID, int start_time, int end_time, gcs::Client* client) {
    bool trace_found = false;
    for (int i=0; i<10; i++) {
        if (trace_found) {
            break;
        }
        for (int j=0; j<10; j++) {
          if (trace_found) {
            break;
          }
          std::string obj_name = std::to_string(i) + std::to_string(j) + "-";
          obj_name += std::to_string(start_time) + "-" + std::to_string(end_time);
          auto reader = client->ReadObject(trace_struct_bucket, obj_name);
          if (reader.status().code() == ::google::cloud::StatusCode::kNotFound) {
            continue;
          } else if (!reader) {
            std::cerr << "Error reading object: " << reader.status() << "\n";
            return 1;
          } else {
            std::string contents{std::istreambuf_iterator<char>{reader}, {}};
            int traceID_location = contents.find(traceID);
            if (traceID_location) {
                trace_found = true;
                int end = contents.find("Trace ID", traceID_location-1);
                if (end) {
                    std::string spans = contents.substr(traceID_location, end-traceID_location);
                    std::vector<std::string> split_spans = split_string_by_newline(spans);
                    // start at 1 because first line will be trace ID
                    for (int k = 1; k < split_spans.size(); k++) {
                        if (split_spans[k] != "") {
                            std::vector<std::string> span_info = split_string_by_colon(split_spans[k]);
                            opentelemetry::proto::trace::v1::Span sp = get_span(i, j, span_info[2], std::to_string(start_time), std::to_string(end_time), client, span_info[1]);
                        }

                    }


                } else { std::cerr << "couldn't find Trace ID" << std::endl; }
            }
            
          }
        }
    }
    return 0;
}

int main(int argc, char* argv[]) {
    // Create a client to communicate with Google Cloud Storage. This client
    // uses the default configuration for authentication and project id.
    auto client = gcs::Client();
    return get_trace("366ada8fbc705fbddf0468d1df1e746f", 1651073970, 1651073970, &client);
}
