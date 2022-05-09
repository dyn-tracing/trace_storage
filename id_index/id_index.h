// Everything related to creating the trace ID index
#ifndef ID_INDEX_H_                                                          
#define ID_INDEX_H_ 

#include <time.h>                                                               
#include <regex>                                                                
#include <string>                                                               
#include <boost/regex.hpp>                                                      
#include "google/cloud/storage/client.h"                                        
#include "opentelemetry/proto/trace/v1/trace.pb.h"                              
#include "bloom_filter.hpp"                                                     
#include <boost/algorithm/string/regex.hpp>                                     
                                                                                
namespace gcs = ::google::cloud::storage;                                       
const char trace_struct_bucket[] = "dyntraces-snicket4";                        

std::vector<std::string> split_string_by_char(const std::string& str, std::string& ch);
std::vector<std::string> generate_prefixes(time_t earliest, time_t latest);
std::vector<std::string> get_batches_between_timestamps(gcs::Client* client, time_t earliest, time_t latest);
bloom_filter create_bloom_filter(gcs::Client* client, std::string batch, time_t earliest, time_t latest);
int bubble_up_bloom_filter(gcs::Client* client, bloom_filter bf);
int update_index(gcs::Client* client, time_t last_updated);

#endif
