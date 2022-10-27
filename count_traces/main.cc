#include "count.h"

int main(int argc, char* argv[]) {
    auto client = gcs::Client();
    count_spans_and_traces(&client);
}
