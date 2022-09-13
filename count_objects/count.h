#ifndef  MICROSERVICES_ENV_TRACE_STORAGE_COUNT_H_ // NOLINT         
#define  MICROSERVICES_ENV_TRACE_STORAGE_COUNT_H_ // NOLINT    

#include <stdlib.h>
#include <string>
#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include "common.h"

int count_objects_in_bucket(std::string bucket_name, gcs::Client* client);
int count_objects(gcs::Client* client);
#endif
