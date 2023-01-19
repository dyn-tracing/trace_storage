#include "get_traces_by_structure.h"

StatusOr<traces_by_structure> get_traces_by_structure(
    trace_structure query_trace, int start_time, int end_time, bool verbose, gcs::Client* client) {
    boost::posix_time::ptime start, stop, start_retrieve_prefixes, start_get_batches;
    start = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration dur;
    print_update("Time to initialize thread pool: " + std::to_string(dur.total_milliseconds()) + "\n", verbose);


    std::vector<std::future<StatusOr<std::vector<traces_by_structure>>>> response_futures;
    StatusOr<std::vector<traces_by_structure>> data_fulfilling_query = filter_data_by_query(
        query_trace, start_time, end_time, verbose, client);
    if (!data_fulfilling_query.ok()) {
        std::cerr << "couldn't get data fulflling query" << std::endl;
        return data_fulfilling_query.status();
    }

    traces_by_structure to_return;

    for (int64_t i=0; i < data_fulfilling_query->size(); i++) {
        merge_traces_by_struct(data_fulfilling_query.value()[i], &to_return);

    }
    stop = boost::posix_time::microsec_clock::local_time();
    dur = stop - start;
    print_update("Time to go through structural filter: " + std::to_string(dur.total_milliseconds()) + "\n", verbose);

    return to_return;
}

std::string get_root_service_name(const std::string &trace) {
    for (const std::string& line : split_by_string(trace, newline)) {
        if (line.substr(0, 1) == ":") {
            return split_by_string(line, colon)[2];
        }
    }
    return "";
}


// Returns empty string if doesn't fit query
StatusOr<traces_by_structure> read_object_and_determine_if_fits_query(trace_structure &query_trace,
    std::string &bucket_name, std::string object_name, std::vector<std::string> &all_object_names,
    time_t start_time, time_t end_time, bool verbose, gcs::Client* client) {

    boost::posix_time::ptime start, stop, start_batches;
    start = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration dur;

    traces_by_structure ts;
    StatusOr<std::string> trace = read_object(bucket_name, object_name, client);
    if (!trace.ok()) {
        std::cout << "could not read trace" << "with bucket name " << bucket_name << "and object name " << object_name << std::endl;
        std::cout << "trace status code " << trace.status().code() << std::endl;
        return trace.status();
    }
    std::string cleaned_trace = strip_from_the_end(trace.value().substr(0, trace->size()), '\n');
    auto valid = check_examplar_validity(cleaned_trace, query_trace, ts);
    if (!valid.ok()) {
        return valid.status();
    }
    if (!valid.value()) {
        traces_by_structure empty;
        return empty;
    }
    auto root_service_name = get_root_service_name(trace.value());
    start_batches= boost::posix_time::microsec_clock::local_time();
    for (auto batch_name : all_object_names) {
        auto status = get_traces_by_structure_data(
            client, object_name, batch_name,
            root_service_name, start_time, end_time, ts);

        if (!status.ok()) {
            return status;
        }
    }
    stop = boost::posix_time::microsec_clock::local_time();
    dur = stop-start;
    print_update("Time for read object and get data: " + std::to_string(dur.total_milliseconds()) + "\n", verbose);
    return ts;
}

std::unordered_set<std::string> get_hashes_for_microservice_with_prefix(std::string microservice,
    std::string prefix, gcs::Client* client) {

    std::string hash_by_microservice_bucket_name = std::string(HASHES_BY_SERVICE_BUCKET_PREFIX) + BUCKETS_SUFFIX;
    std::unordered_set<std::string> to_return;
    for (auto&& object_metadata : client->ListObjects(hash_by_microservice_bucket_name,
        gcs::Prefix(microservice+"/"+prefix))) {
        if (!object_metadata) {
            throw std::runtime_error(object_metadata.status().message());
        }
        // Okay, now read everything here.
        // name is service / hash
        StatusOr<std::string> hashes = read_object(
            hash_by_microservice_bucket_name, object_metadata->name(), client);
        if (!hashes) {
            std::cerr << "problem" << std::endl;
        }
        for (auto &hash : split_by_string(hashes.value(), newline)) {
            if (hash != "") {
                to_return.insert(hash);
            }
        }
    }
    return to_return;
}


std::unordered_set<std::string> get_hashes_for_microservice(std::string microservice, gcs::Client* client) {
    std::unordered_set<std::string> to_return;
    boost::posix_time::ptime start, stop, start_list_objects;
    boost::posix_time::time_duration dur;

    start = boost::posix_time::microsec_clock::local_time();
    BS::thread_pool pool(20);
    std::vector<std::future<std::unordered_set<std::string>>> future_hashes;
    for (int64_t i = 0; i < 10; i++) {
        for (int64_t j = 0; j < 10; j++) {
            std::string new_prefix = std::to_string(i) + std::to_string(j);
            future_hashes.push_back(pool.submit(get_hashes_for_microservice_with_prefix,
                microservice, new_prefix, client));
        }
    }
    for (int64_t i=0; i < future_hashes.size(); i++) {
        for (auto &hash : future_hashes[i].get()) {
            to_return.insert(hash);
        }
    }

    stop = boost::posix_time::microsec_clock::local_time();
    dur = stop-start;
    std::cout << "time for listing hashes of microservices : " <<  dur.total_milliseconds() << std::endl;
    //print_update("Time for listing hashes of microservice: " + std::to_string(dur.total_milliseconds()) + "\n", verbose);
    return to_return;
}

std::vector<std::string> get_potential_prefixes(trace_structure &query_trace, bool verbose, gcs::Client* client) {
    boost::posix_time::ptime start, stop, start_list_objects;
    start = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration dur;
    // If we know one or more microservice names, find their hashes and
    // intersect them.
    std::vector<std::string> service_names;
    for (auto &pair : query_trace.node_names) {
        if (pair.second != "*") {
            service_names.push_back(pair.second);
        }
    }

    if (service_names.size() == 0) {
        std::string list_bucket_name = std::string(LIST_HASHES_BUCKET_PREFIX) + BUCKETS_SUFFIX;
        // If we do not know any microservice names, simply return all objects
        // in the list bucket.
        start_list_objects = boost::posix_time::microsec_clock::local_time();
        std::vector<std::string> bucket_objs = list_objects_in_bucket(client, list_bucket_name);
        stop = boost::posix_time::microsec_clock::local_time();
        dur = stop-start_list_objects;
        print_update("Time to list: " + std::to_string(dur.total_milliseconds()) + "\n", verbose);
        return bucket_objs;
    }

    BS::thread_pool pool(20);
    std::vector<std::future<std::unordered_set<std::string>>> future_hashes;
    for (std::string& service_name : service_names) {
        future_hashes.push_back(pool.submit(get_hashes_for_microservice, service_name, client));
    }
    std::unordered_map<std::string, int64_t> hash_to_count;
    for (int64_t i=0; i < future_hashes.size(); i++) {
        for (auto & hash : future_hashes[i].get()) {
            if (hash_to_count.count(hash) == 0) {
                hash_to_count[hash] = 1;
            } else {
                hash_to_count[hash]++;
            }
        }
    }

    std::vector<std::string> to_return;
    // get hashes from hash to count
    for (auto & pair : hash_to_count) {
        if (pair.second == service_names.size()) {
            to_return.push_back(pair.first);
        }
    }
    return to_return;
}

StatusOr<std::vector<traces_by_structure>> filter_data_by_query(trace_structure &query_trace, time_t start_time, time_t end_time, bool verbose, gcs::Client* client) {
    std::string list_bucket_name = std::string(LIST_HASHES_BUCKET_PREFIX) + BUCKETS_SUFFIX;

    boost::posix_time::ptime start, stop, start_batches, start_list_objects;
    start = boost::posix_time::microsec_clock::local_time();
    boost::posix_time::time_duration dur;

    start = boost::posix_time::microsec_clock::local_time();
    std::vector<std::string> all_object_names = get_batches_between_timestamps(client, start_time, end_time);
    stop = boost::posix_time::microsec_clock::local_time();
    dur = stop-start;
    print_update("Time to get all object names: " + std::to_string(dur.total_milliseconds()) + "\n", verbose);
    BS::thread_pool pool(200);
    std::vector<traces_by_structure> to_return;

    // here, we list all the objects in the bucket, because there is only one
    // object per prefix
    std::vector<std::string> bucket_objs = get_potential_prefixes(query_trace, verbose, client);
    std::vector<std::future<StatusOr<traces_by_structure>>> future_traces_by_struct;


    // Here, rather than listing all objects in the list bucket, we instead
    // find the hashes by the microservice names.
    for (std::string& prefix : bucket_objs) {
        future_traces_by_struct.push_back(pool.submit(read_object_and_determine_if_fits_query,
            std::ref(query_trace), std::ref(list_bucket_name), prefix, std::ref(all_object_names), start_time, end_time, verbose, client
        ));
    }
    /*
    for (int i=0; i<5; i++) {
	    std::cout << "tasks queued: " << pool.get_tasks_queued() << std::endl;
	    std::cout << "tasks running: " << pool.get_tasks_running() << std::endl;
	    sleep(1);
    }
    */
    for (int64_t i=0; i < future_traces_by_struct.size(); i++) {
        StatusOr<traces_by_structure> cur_traces_by_structure = future_traces_by_struct[i].get();
        if (!cur_traces_by_structure.ok()) {
            std::cerr << "sad, there's been an error" << std::endl;
            return cur_traces_by_structure.status();
        }
        to_return.push_back(cur_traces_by_structure.value());
    }
    stop = boost::posix_time::microsec_clock::local_time();
    dur = stop-start;
    print_update("Time for filter data by query: " + std::to_string(dur.total_milliseconds()) + "\n", verbose);
    return to_return;
}

void merge_traces_by_struct(const traces_by_structure &new_trace_by_struct, traces_by_structure* old ) {
    int trace_id_offset = old->trace_ids.size();
    int iso_map_offset = old->iso_maps.size();

    // first merge the vectors of the data itself
    old->trace_ids.insert(old->trace_ids.end(),
                                new_trace_by_struct.trace_ids.begin(),
                                new_trace_by_struct.trace_ids.end());
    old->object_names.insert(old->object_names.end(),
                                    new_trace_by_struct.object_names.begin(),
                                    new_trace_by_struct.object_names.end());
    old->iso_maps.insert(old->iso_maps.end(),
                                new_trace_by_struct.iso_maps.begin(),
                                new_trace_by_struct.iso_maps.end());
    old->trace_node_names.insert(old->trace_node_names.end(),
                                        new_trace_by_struct.trace_node_names.begin(),
                                        new_trace_by_struct.trace_node_names.end());

    // now merge the pointers by adding the offsets to everything
    for (const auto &pair : new_trace_by_struct.object_name_to_trace_ids_of_interest) {
        auto object_name = pair.first;
        for (uint64_t i=0; i < pair.second.size(); i++) {
            old->object_name_to_trace_ids_of_interest[object_name].push_back(
                pair.second[i] + trace_id_offset);
        }
    }

    for (const auto &pair : new_trace_by_struct.trace_id_to_isomap_indices) {
        std::vector<int> isomap_indices;
        for (uint64_t i=0; i < pair.second.size(); i++) {
            isomap_indices.push_back(pair.second[i] + iso_map_offset);
        }
        old->trace_id_to_isomap_indices[pair.first] = isomap_indices;
    }

    // then finally deal with trace node name stuff
    if (new_trace_by_struct.trace_node_names.size() > 0) {
        old->trace_node_names.push_back(new_trace_by_struct.trace_node_names[0]);
        int tnn_index = old->trace_node_names.size()-1;
        for (uint64_t i=iso_map_offset; i < old->iso_maps.size(); i++) {
            old->iso_map_to_trace_node_names[i] = tnn_index;
        }
    }
}

StatusOr<std::string> get_examplar_from_prefix(std::string prefix, gcs::Client* client) {
    std::string prefix_to_search = std::string(TRACE_HASHES_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX);
    std::string object_name = "";

    for (auto&& object_metadata : client->ListObjects(prefix_to_search, gcs::Prefix(prefix), gcs::MaxResults(1))) {
        if (!object_metadata) {
            std::cerr << object_metadata.status().message() << std::endl;
            return object_metadata.status();
        }
        object_name =  object_metadata->name();

        // MaxResults param just limits the size of the page, we do not need
        // to iterate over more data, so break...
        break;
    }

    auto response_trace_ids_or_status = get_trace_ids_from_trace_hashes_object(object_name, client);
    if (!response_trace_ids_or_status.ok()) {
        return response_trace_ids_or_status.status();
    }

    auto response_trace_ids = response_trace_ids_or_status.value();

    std::string batch_name = extract_batch_name(object_name);
    auto object_content_or_status = read_object(TRACE_STRUCT_BUCKET_PREFIX+std::string(BUCKETS_SUFFIX),
        batch_name, client);
    if (!object_content_or_status.ok()) {
        return Status(google::cloud::StatusCode::kUnavailable, "could not get examplar");
    }

    auto object_content = object_content_or_status.value();
    std::string trace = extract_any_trace(response_trace_ids, object_content);
    return trace;
}

/**
 * Checks examplar validity and sets isomaps and node_names in the passed by reference param to_return.
*/
StatusOr<bool> check_examplar_validity(
    std::string examplar, trace_structure query_trace, traces_by_structure& to_return) {
    trace_structure candidate_trace = morph_trace_object_to_trace_structure(examplar);

    auto iso_mappings = get_isomorphism_mappings(candidate_trace, query_trace);
    if (iso_mappings.size() < 1) {
        return false;
    }

    // Store Isomaps
    to_return.iso_maps = iso_mappings;

    // Store Node Names
    std::vector<std::unordered_map<int, std::string>> nn;
    nn.push_back(candidate_trace.node_names);
    to_return.trace_node_names = nn;

    return true;
}

Status get_traces_by_structure_data(
    gcs::Client* client,
    std::string prefix,
    std::string batch_name,
    std::string root_service_name,
    time_t start_time, time_t end_time,
    traces_by_structure& to_return
) {
    auto hashes_bucket_object_name = prefix + "/" + batch_name;

    if (false == is_object_within_timespan(extract_batch_timestamps(batch_name), start_time, end_time)) {
        return Status();
    }

    auto response_trace_ids_or_status = get_trace_ids_from_trace_hashes_object(hashes_bucket_object_name, client);

    if (!response_trace_ids_or_status.ok()) {
        return Status(
            google::cloud::StatusCode::kUnavailable,
            "error in get_traces_by_structure_data: get_trace_ids_from_trace_hashes_object");
    }

    auto response_trace_ids = response_trace_ids_or_status.value();
    if (response_trace_ids.size() < 1) {
        return Status();
    }

    auto trace_ids_to_append = response_trace_ids;

    if (object_could_have_out_of_bound_traces(extract_batch_timestamps(batch_name), start_time, end_time)) {
        auto trace_ids_to_append_or_status = filter_trace_ids_based_on_query_timestamp_for_given_root_service(
            response_trace_ids, batch_name, start_time, end_time, root_service_name, client);
        if (!trace_ids_to_append_or_status.ok()) {
            return trace_ids_to_append_or_status.status();
        }
        trace_ids_to_append = trace_ids_to_append_or_status.value();
    }

    int trace_id_offset = to_return.trace_ids.size();
    to_return.trace_ids.insert(to_return.trace_ids.end(),
        trace_ids_to_append.begin(), trace_ids_to_append.end());

    to_return.object_names.push_back(batch_name);
    int batch_name_index = to_return.object_names.size()-1;
    for (uint64_t i=trace_id_offset; i < to_return.trace_ids.size(); i++) {
        for (uint64_t j=0; j < to_return.iso_maps.size(); j++) {
            to_return.trace_id_to_isomap_indices[to_return.trace_ids[i]].push_back(j);
        }
        to_return.object_name_to_trace_ids_of_interest[batch_name_index].push_back(i);
    }

    return Status();
}

StatusOr<potential_prefix_struct> get_potential_prefixes(
    std::string prefix, gcs::Client* client) {
    std::string prefix_to_search = std::string(TRACE_HASHES_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX);
    std::string object_name = "";

    for (auto&& object_metadata : client->ListObjects(prefix_to_search, gcs::Prefix(prefix), gcs::MaxResults(1))) {
        if (!object_metadata) {
            std::cerr << object_metadata.status().message() << std::endl;
            return object_metadata.status();
        }
        object_name =  object_metadata->name();

        // MaxResults param just limits the size of the page, we do not need
        // to iterate over more data, so break...
        break;
    }

    StatusOr<std::string> trace_id = get_single_trace_id_from_trace_hashes_object(object_name, client);
    if (!trace_id.ok()) {
        std::cerr << "could not get single race ID from hashes object" << std::endl;
        return trace_id.status();
    }

    return potential_prefix_struct {
        .prefix = prefix,
        .batch_name = extract_batch_name(object_name),
        .trace_id = trace_id.value(),
    };
}

StatusOr<traces_by_structure> filter_prefix_by_query(std::string &batch_name, std::string &prefix,
    std::string &trace_id,
    std::string &object_content, trace_structure &query_trace, int start_time, int end_time,
    const std::vector<std::string> &all_object_names, bool verbose, gcs::Client* client) {
    traces_by_structure cur_traces_by_structure;
    std::string trace = extract_trace_from_traces_object(trace_id, object_content);
    if (trace == "") {
        std::cerr << "problematic" << std::endl;
        return Status();  // TODO(jessberg): what is error code
    }
    auto valid = check_examplar_validity(trace, query_trace, cur_traces_by_structure);
    if (!valid.ok()) {
        return valid.status();
    }

    if (!valid.value()) {
        traces_by_structure empty;
        return empty;
    }

    auto root_service_name = get_root_service_name(trace);
    for (auto batch_name : all_object_names) {
        auto status = get_traces_by_structure_data(
            client, prefix, batch_name,
            root_service_name, start_time, end_time, cur_traces_by_structure);

        if (!status.ok()) {
            return status;
        }
    }
    return cur_traces_by_structure;
}

StatusOr<std::vector<traces_by_structure>> filter_by_query(std::string batch_name,
    std::vector<std::pair<std::string, std::string>> &prefix_to_trace_ids,
    trace_structure query_trace, int start_time, int end_time,
    const std::vector<std::string>& all_object_names, bool verbose, gcs::Client* client) {

    boost::posix_time::ptime start, stop;
    boost::posix_time::time_duration dur;

    start = boost::posix_time::microsec_clock::local_time();
    std::vector<traces_by_structure> to_return;
    auto object_content_or_status = read_object(TRACE_STRUCT_BUCKET_PREFIX+std::string(BUCKETS_SUFFIX),
        batch_name, client);
    if (!object_content_or_status.ok()) {
        return Status(google::cloud::StatusCode::kUnavailable, "could not get examplar");
    }
    stop = boost::posix_time::microsec_clock::local_time();
    dur = stop - start;
    print_update("Time to retrieve object: " + std::to_string(dur.total_milliseconds()) + "\n", verbose);
    BS::thread_pool pool(5);  // TODO(jessberg): what is optimum val here?

    auto object_content = object_content_or_status.value();
    std::vector<std::future<StatusOr<traces_by_structure>>> future_traces_by_structure;
    for (int64_t i=0; i < prefix_to_trace_ids.size(); i++) {
        future_traces_by_structure.push_back(pool.submit(filter_prefix_by_query,
            std::ref(batch_name), std::ref(std::get<0>(prefix_to_trace_ids[i])),
            std::ref(std::get<1>(prefix_to_trace_ids[i])), std::ref(object_content),
            std::ref(query_trace), start_time, end_time,
            all_object_names, verbose, client));
    }
    StatusOr<traces_by_structure> cur_traces_by_structure;
    for (int64_t i=0; i < future_traces_by_structure.size(); i++) {
        auto cur_traces_by_structure = future_traces_by_structure[i].get();
        if (!cur_traces_by_structure.ok()) {
            std::cerr << "oh no!" << std::endl;
        }

        to_return.push_back(cur_traces_by_structure.value());
    }
    stop = boost::posix_time::microsec_clock::local_time();
    dur = stop - start;
    print_update("Time to filter by query: " + std::to_string(dur.total_milliseconds()) + "\n", verbose);
    return to_return;
}


StatusOr<std::vector<std::string>> filter_trace_ids_based_on_query_timestamp_for_given_root_service(
    std::vector<std::string> &trace_ids,
    std::string &batch_name,
    const int start_time,
    const int end_time,
    std::string &root_service_name,
    gcs::Client* client) {
    std::vector<std::string> response;

    auto spans_data = read_object(root_service_name + std::string(BUCKETS_SUFFIX), batch_name, client);
    if (!spans_data.ok()) {
        return spans_data.status();
    }

    std::map<std::string, std::pair<int, int>> trace_id_to_timestamp_map = get_timestamp_map_for_trace_ids(
        spans_data.value(), trace_ids);

    for (const auto& trace_id : trace_ids) {
        std::pair<int, int> trace_timestamp = trace_id_to_timestamp_map[trace_id];
        if (is_object_within_timespan(trace_timestamp, start_time, end_time)) {
            response.push_back(trace_id);
        }
    }

    return response;
}

/**
 * @brief Get the isomorphism mappings object
 *
 * Map: query trace => stored trace
 *
 * @param candidate_trace
 * @param query_trace
 * @return std::vector<std::unordered_map<int, int>>
 */
std::vector<std::unordered_map<int, int>> get_isomorphism_mappings(
    trace_structure &candidate_trace, trace_structure &query_trace) {

    graph_type candidate_graph = morph_trace_structure_to_boost_graph_type(candidate_trace);
    graph_type query_graph = morph_trace_structure_to_boost_graph_type(query_trace);

    vertex_comp_t vertex_comp = make_property_map_equivalent_custom(
        boost::get(boost::vertex_name_t::vertex_name, query_graph),
        boost::get(boost::vertex_name_t::vertex_name, candidate_graph));

    std::vector<std::unordered_map<int, int>> isomorphism_maps;

    vf2_callback_custom<graph_type, graph_type, std::vector<std::unordered_map<int, int>>> callback(
        query_graph, candidate_graph, isomorphism_maps);

    boost::vf2_subgraph_iso(
        query_graph,
        candidate_graph,
        callback,
        boost::vertex_order_by_mult(query_graph),
        boost::vertices_equivalent(vertex_comp));
    return isomorphism_maps;
}

StatusOr<std::string> get_single_trace_id_from_trace_hashes_object(
    const std::string &object_name, gcs::Client* client) {

    auto reader = client->ReadObject(std::string(TRACE_HASHES_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX),
        object_name, gcs::ReadRange(0, TRACE_ID_LENGTH+1));
    if (!reader) {
        return reader.status();
    }

    std::string object_content{std::istreambuf_iterator<char>{reader}, {}};

    if (object_content == "") {
        return std::string("");
    }
    object_content.erase(std::remove_if(object_content.begin(), object_content.end(), ::isspace), object_content.end());
    return object_content;
}

StatusOr<std::vector<std::string>> get_trace_ids_from_trace_hashes_object(
    const std::string &object_name, gcs::Client* client) {
    auto object_content = read_object(
        std::string(TRACE_HASHES_BUCKET_PREFIX) + std::string(BUCKETS_SUFFIX),
        object_name, client);
    if (!object_content.ok()) {
        if (object_content.status().code() == google::cloud::StatusCode::kNotFound) {
            std::vector<std::string> res;
            return res;
        }
        return object_content.status();
    }

    if (object_content.value() == "") {
        return std::vector<std::string>();
    }
    std::vector<std::string> response;
    for (auto curr_trace_id : split_by_string(object_content.value(), newline)) {
        if (curr_trace_id != "") {
            response.push_back(curr_trace_id);
        }
    }
    return response;
}

trace_structure morph_trace_object_to_trace_structure(std::string &trace) {
    trace_structure response;

    std::unordered_map<std::string, std::string> span_to_service;
    std::unordered_map<std::string, int> reverse_node_names;
    std::multimap<std::string, std::string> edges;

    for (std::string& line : split_by_string(trace, newline)) {
        if (line.substr(0, 10) == "Trace ID: ") {
            continue;
        }

        std::vector<std::string> span_info = split_by_string(line, colon);
        if (span_info.size() != 4) {
            std::cerr << "Malformed trace found: \n" << trace << std::endl;
            std::cerr << "Line was : \n" << line << std::endl;
            return response;
        }

        span_to_service.insert(std::make_pair(span_info[1], span_info[1]+":"+span_info[2]+":"+span_info[3]));

        if (span_info[0].length() > 0) {
            edges.insert(std::make_pair(span_info[0], span_info[1]));
        }
    }

    response.num_nodes = span_to_service.size();

    // Filling response.node_names
    int count = 0;
    for (const auto& elem : span_to_service) {
        response.node_names.insert(make_pair(count, elem.second));
        reverse_node_names.insert(make_pair(elem.second, count));
        count++;
    }

    // Filling response.edges
    for (const auto& elem : edges) {
        response.edges.insert(std::make_pair(
            reverse_node_names[span_to_service[elem.first]],
            reverse_node_names[span_to_service[elem.second]]));
    }

    for (auto [k, v] : response.node_names) {
        response.node_names[k] = split_by_string(v, colon)[1];
    }

    return response;
}

graph_type morph_trace_structure_to_boost_graph_type(trace_structure &input_graph) {
    graph_type output_graph;

    for (uint64_t i = 0; i < input_graph.num_nodes; i++) {
        boost::add_vertex(vertex_property(input_graph.node_names[i], i), output_graph);
    }

    for (const auto& elem : input_graph.edges) {
        boost::add_edge(elem.first, elem.second, output_graph);
    }

    return output_graph;
}

void print_trace_structure(trace_structure trace) {
    std::cout << "n: " << trace.num_nodes << std::endl;
    std::cout << "node names:" << std::endl;
    for (const auto& elem : trace.node_names) {
        std::cout << elem.first << " : " << elem.second << std::endl;
    }
    std::cout << "edges:" << std::endl;
    for (const auto& elem : trace.edges) {
        std::cout << elem.first << " : " << elem.second << std::endl;
    }
}
