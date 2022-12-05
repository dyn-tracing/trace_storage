#include "range_index.h"

int main(int argc, char* argv[]) {
    auto client = gcs::Client();
    Status ret = update("latency", &client);
}
