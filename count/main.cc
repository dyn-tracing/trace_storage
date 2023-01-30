#include "count_traces.h"
#include "count_objects.h"

int main(int argc, char* argv[]) {
    auto client = gcs::Client();
    // count_spans_and_traces(&client);
    const int64_t res = count_objects(&client, true);
    std::cout << "Total data size " << res << std::endl;
    const int64_t res2 = count_objects(&client, false);
    std::cout << "Total objects " << res2 << std::endl;
}
