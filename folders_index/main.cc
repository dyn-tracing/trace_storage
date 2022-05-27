#include "folders_index.h"

int main(int argc, char* argv[]) {
	dummy_tests();

	auto client = gcs::Client();

	std::string indexed_attribute = "http.status_code";
	create_index_bucket_if_not_present(indexed_attribute, &(client));
	time_t last_updated = get_last_updated_for_bucket(get_bucket_name_for_attr(
		indexed_attribute), &(client));

	boost::posix_time::ptime start, stop;
	start = boost::posix_time::microsec_clock::local_time();

	update_index(&(client), last_updated, indexed_attribute);

	stop = boost::posix_time::microsec_clock::local_time();

	boost::posix_time::time_duration dur = stop - start;
	int64_t milliseconds = dur.total_milliseconds();
	std::cout << "Time taken: " << milliseconds << std::endl;
	return 0;
}
