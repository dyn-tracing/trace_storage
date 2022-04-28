#include "google/cloud/storage/client.h"
#include "opentelemetry/proto/trace/v1/trace.pb.h"
#include <boost/algorithm/string.hpp>
#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/vf2_sub_graph_iso.hpp>
#include <iostream>

const std::string TRACE_STRUCT_BUCKET = "dyntraces-snicket2";
const std::string TRACE_HASHES_BUCKET = "tracehashes-snicket2";
const std::string SERVICES_BUCKETS_SUFFIX = "-snicket2";
const std::string ASTERISK_SERVICE = "NONE";

const int TRACE_ID_LENGTH = 32;
const int SPAN_ID_LENGTH = 16;

namespace gcs = ::google::cloud::storage;
namespace bg = boost::graph;

struct trace_structure {
	int num_nodes;
	std::unordered_map<int, std::string> node_names;
	std::multimap<int, int> edges;
};

// Binary function object that returns true if the values for item1
// in property_map1 and item2 in property_map2 are equivalent.
template < typename PropertyMapFirst, typename PropertyMapSecond >
struct property_map_equivalent_custom {

    property_map_equivalent_custom(const PropertyMapFirst property_map1,
        const PropertyMapSecond property_map2)
    : m_property_map1(property_map1), m_property_map2(property_map2) {
    }

    template < typename ItemFirst, typename ItemSecond >
    bool operator()(const ItemFirst item1, const ItemSecond item2) {
        if (get(m_property_map1, item1) == ASTERISK_SERVICE || get(m_property_map2, item2) == ASTERISK_SERVICE) {
            return true;
        }

        return (get(m_property_map1, item1) == get(m_property_map2, item2));
    }

private:
    const PropertyMapFirst m_property_map1;
    const PropertyMapSecond m_property_map2;
};

template < typename PropertyMapFirst, typename PropertyMapSecond >
property_map_equivalent_custom< PropertyMapFirst, PropertyMapSecond > make_property_map_equivalent_custom(
    const PropertyMapFirst property_map1, const PropertyMapSecond property_map2
) {
    return (property_map_equivalent_custom< PropertyMapFirst, PropertyMapSecond >(
        property_map1, property_map2));
}

template < typename Graph1, typename Graph2 > 
struct vf2_callback_custom {
    vf2_callback_custom(const Graph1& graph1, const Graph2& graph2)
    : graph1_(graph1), graph2_(graph2)
    {
    }

    /**
     * @brief Returning false so that the isomorphism finding process stops after finding 
     * a single isomorphism evidence. 
     */
    template < typename CorrespondenceMap1To2, typename CorrespondenceMap2To1 >
    bool operator()(CorrespondenceMap1To2 f, CorrespondenceMap2To1) const {
        return false;
    }

private:
    const Graph1& graph1_;
    const Graph2& graph2_;
};

typedef boost::property<boost::vertex_name_t, std::string, boost::property<boost::vertex_index_t, int> > vertex_property;
typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS, vertex_property> graph_type;
typedef boost::property_map<graph_type, boost::vertex_name_t>::type vertex_name_map_t;
typedef property_map_equivalent_custom<vertex_name_map_t, vertex_name_map_t> vertex_comp_t;

std::string hex_str(std::string data, int len);
std::map<std::string, std::pair<int, int>> get_timestamp_map_for_trace_ids(std::string spans_data, std::vector<std::string> trace_ids);
std::map<std::string, std::vector<std::string>> get_root_service_to_trace_ids_map(std::map<std::string, std::string> trace_id_to_root_service_map);
std::map<std::string, std::string> get_trace_id_to_root_service_map(std::string object_content);
std::vector<std::string> filter_trace_ids_based_on_query_timestamp(std::vector<std::string> trace_ids, std::string batch_name, std::string object_content, int start_time, int end_time, gcs::Client* client);
graph_type morph_trace_structure_to_boost_graph_type(trace_structure input_graph);
void print_trace_structure(trace_structure trace);
std::string extract_trace_from_traces_object(std::string trace_id, std::string object_content);
std::pair<int, int> extract_batch_timestamps(std::string batch_name); 
std::string extract_batch_name(std::string object_name);
bool does_trace_structure_conform_to_graph_query( std::string object_content, std::string trace_id, trace_structure query_trace, gcs::Client* client);
std::vector<std::string> split_by_char(std::string input, std::string splitter);
std::vector<std::string> split_by_line(std::string input);
bool is_object_within_timespan(std::pair<int, int> batch_time, int start_time, int end_time);
std::string read_object(std::string bucket, std::string object, gcs::Client* client);
std::vector<std::string> get_trace_ids_from_trace_hashes_object(std::string object_name, gcs::Client* client);
int get_trace(std::string traceID, int start_time, int end_time, gcs::Client* client); 
int get_traces_by_structure(trace_structure query_trace, int start_time, int end_time, gcs::Client* client);
std::string strip_from_the_end(std::string object, char stripper);
trace_structure morph_trace_object_to_trace_structure(std::string trace);
bool is_isomorphic(trace_structure query_trace, trace_structure candidate_trace);
int dummy_tests();