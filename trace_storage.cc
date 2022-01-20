// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <string>
#include "google/cloud/storage/client.h"

namespace gcs = ::google::cloud::storage;

int write_object(gcs::Client &client, std::string bucket_name, std::string object_name, std::string content) {
    auto writer = client.WriteObject(bucket_name, object_name);
    writer << content;
    writer.Close();
    if (!writer.metadata()) {
      std::cerr << "Error creating object: " << writer.metadata().status()
                << "\n";
      return 1;
    }
    return 0;
}

std::string read_object(gcs::Client &client, std::string bucket_name, std::string object_name) {
  auto reader = client.ReadObject(bucket_name, object_name);
  if (!reader) {
    std::cerr << "Error reading object: " << reader.status() << "\n";
  }

  std::string contents{std::istreambuf_iterator<char>{reader}, {}};
  std::cout << contents << "\n";
  return contents;
}

int delete_object(gcs::Client &client, std::string bucket_name, std::string object_name) {
  google::cloud::Status status =
        client.DeleteObject(bucket_name, object_name);
  if (!status.ok()) {
    std::cerr << "Error deleting object: " << status << "\n";
  }
  return 0;
}

int create_bucket(gcs::Client &client, std::string bucket_name, std::string project_id) {
  google::cloud::StatusOr<gcs::BucketMetadata> bucket_metadata =
      client.CreateBucketForProject(
          bucket_name, project_id,
          gcs::BucketMetadata()
              .set_location("us-east1")
              .set_storage_class(gcs::storage_class::Regional()));
  if (!bucket_metadata) {
    std::cerr << "Error creating bucket " << bucket_name
              << ", status=" << bucket_metadata.status() << "\n";
    return 1;
  }
  return 0;

}

int delete_bucket(gcs::Client &client, std::string bucket_name) {
  google::cloud::Status status = client.DeleteBucket(bucket_name);
  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
  return 0;
}


int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Missing bucket name.\n";
    std::cerr << "Usage: quickstart <bucket-name>\n";
    return 1;
  }
  std::string const bucket_name = argv[1];

  // Create aliases to make the code easier to read.

  // Create a client to communicate with Google Cloud Storage. This client
  // uses the default configuration for authentication and project id.
  auto client = gcs::Client();
  create_bucket(client, "trace_storage_bucket", "dynamic-tracing");
  write_object(client, "trace_storage_bucket", "new_object", "Here is a sentence"); 
  std::string written = read_object(client, "trace_storage_bucket", "new_object");
  if (written == "Here is a sentence") {
    std::cout << "Got correct bucket data" << std::endl;
  } else {
    std::cout << "Bucket data was instead " << written << std::endl;
  }
  delete_object(client, "trace_storage_bucket", "new_object");
  delete_bucket(client, "trace_storage_bucket");
  std::cout << "All actions done" << std::endl;


  return 0;
}
