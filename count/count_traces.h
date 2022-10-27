#ifndef  COUNT_COUNT_TRACES_H_ // NOLINT         
#define  COUNT_COUNT_TRACES_H_ // NOLINT    

#include <stdlib.h>
#include <string>
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "common.h"

void count_spans_and_traces(gcs::Client* client);

#endif  // COUNT_COUNT_TRACES_H_
