#include "count_objects/count.h"


std::string SUFFIX = "-snicket2";

int count_objects_in_bucket(std::string bucket_name, gcs::Client* client) {

    int count = 0;
    for (auto& object_metadata : client ->ListObjects(bucket_name)) {
        if (!object_metadata) {
            std::cerr << "Error in getting object" << std::endl;              
            exit(1);
        }

        count++;
    }

    return count;
}

int count_objects(gcs::Client* client) {
    std::vector<std::string> bucket_prefixes = {
        "frontend", "adservice", "cartservice", "checkoutservice", "currencyservice", 
        "emailservice", "paymentservice", "productcatalogservice", "recommendationservice", 
        "rediscart", "shippingservice",
        "dyntraces", "tracehashes"
    };

    auto count = 0;

    std::vector<std::future<int>> response_futures;

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