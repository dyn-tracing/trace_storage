#include "id_index.h"

int main(int argc, char* argv[]) {
    // Create a client to communicate with Google Cloud Storage. This client
    // uses the default configuration for authentication and project id.
    auto client = gcs::Client();
    get_value_func func;
    func.bytes_func = &opentelemetry::proto::trace::v1::Span::parent_span_id;

    update_index(&client, "trace.id", 1000, bytes_value, func);
    return 0;
}
