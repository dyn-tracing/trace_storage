#ifndef  COUNT_COUNT_OBJECTS_H_ // NOLINT
#define  COUNT_COUNT_OBJECTS_H_ // NOLINT

#include <stdlib.h>
#include <string>
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "common.h"

int64_t count_objects_size(std::string bucket_name, gcs::Client* client);
int64_t count_objects_in_bucket(std::string bucket_name, gcs::Client* client);
int64_t count_objects(gcs::Client* client, bool size);

#endif  // COUNT_COUNT_OBJECTS_H_
