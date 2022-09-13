#include "count.h"

int main(int argc, char* argv[]) {
    auto client = gcs::Client();
    auto res = count_objects(&client);
    std::cout << "Total objects " << res << std::endl;
}
