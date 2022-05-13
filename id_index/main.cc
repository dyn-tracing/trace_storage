#include "id_index.h"

int main(int argc, char* argv[]) {
    // Create a client to communicate with Google Cloud Storage. This client
    // uses the default configuration for authentication and project id.
    auto client = gcs::Client();
    time_t last_updated = 1651700400;
    update_index(&client, last_updated);
    return 0;
}
