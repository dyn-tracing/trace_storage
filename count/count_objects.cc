#include "count/count_objects.h"

#include <vector>
#include "common.h"

int64_t count_objects_size(std::string bucket_name, gcs::Client* client) {
    int64_t count = 0;
    for (auto& object_metadata : client ->ListObjects(bucket_name)) {
        if (!object_metadata) {
            std::cerr << "Error in getting object" << std::endl;
            std::cerr << "tried to get from bucket " << bucket_name << std::endl;
            exit(1);
        }
        count += object_metadata->size();
    }
    return count;
}

int64_t count_objects_in_bucket(std::string bucket_name, gcs::Client* client) {
    int64_t count = 0;
    for (auto& object_metadata : client ->ListObjects(bucket_name)) {
        if (!object_metadata) {
            std::cerr << "Error in listing object" << std::endl;
            std::cerr << "tried to get bucket name " << bucket_name << std::endl;
            exit(1);
        }

        count++;
    }

    return count;
}

int64_t count_objects(gcs::Client* client, bool size) {
    std::vector<std::string> bucket_prefixes = {
        "hashes-by-service", "list-hashes", "microservices",
        "dyntraces", "tracehashes"
    };

    int64_t count = 0;

    std::vector<std::future<int64_t>> response_futures;

    for (auto ele : bucket_prefixes) {
        auto bucket_name = ele + BUCKETS_SUFFIX;
        if (size) {
            response_futures.push_back(std::async(std::launch::async, count_objects_size, bucket_name, client));
        } else {
            response_futures.push_back(std::async(std::launch::async, count_objects_in_bucket, bucket_name, client));
        }
    }

    for (int64_t i = 0; i < response_futures.size(); i++) {
        auto res = response_futures[i].get();
        count += res;
    }

    return count;
}
