#include "count.h"

int main(int argc, char* argv[]) {
    auto client = gcs::Client();
    long long int res = count_objects(&client);
    std::cout << "Total data " << res << std::endl;
}
