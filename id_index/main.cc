#include "id_index.h"

int main(int argc, char* argv[]) {
    // Create a client to communicate with Google Cloud Storage. This client
    // uses the default configuration for authentication and project id.
    auto client = gcs::Client();
    get_value_func func;
    func.bytes_func = &opentelemetry::proto::trace::v1::Span::trace_id;

    get_value_func condition_1_union;
    condition_1_union.int_func = &opentelemetry::proto::trace::v1::Span::start_time_unix_nano;
    update_index(&client, "trace.id", 10, bytes_value, func);
    return 0;
}
