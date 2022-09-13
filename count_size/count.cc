#include "count_size/count.h"


std::string SUFFIX = "-snicket2";

long long int count_objects_in_bucket(std::string bucket_name, gcs::Client* client) {

    long long int count = 0;
    for (auto& object_metadata : client ->ListObjects(bucket_name)) {
        if (!object_metadata) {
            std::cerr << "Error in getting object" << std::endl;              
            exit(1);
        }

        count += object_metadata->size();
    }

    return count;
}

long long int count_objects(gcs::Client* client) {
    std::vector<std::string> bucket_prefixes = {
        "frontend", "adservice", "cartservice", "checkoutservice", "currencyservice", 
        "emailservice", "paymentservice", "productcatalogservice", "recommendationservice", 
        "rediscart", "shippingservice",
        "dyntraces", "tracehashes"
    };

    long long int count = 0;

    std::vector<std::future<long long int>> response_futures;

    for (auto ele : bucket_prefixes) {
        auto bucket_name = ele + SUFFIX;

        response_futures.push_back(std::async(std::launch::async, count_objects_in_bucket, bucket_name, client));
    }

    for (int i = 0; i < response_futures.size(); i++) {
        auto res = response_futures[i].get();
        count += res;
    }

    return count;
}