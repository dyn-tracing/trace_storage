#include "id_index.h"

int main(int argc, char* argv[]) {
    // Create a client to communicate with Google Cloud Storage. This client
    // uses the default configuration for authentication and project id.
    auto client = gcs::Client();
    update_index(&client, "new_id_index", 10);
    return 0;
}
