#ifndef BY_STRUCT_H_ // NOLINT
#define BY_STRUCT_H_

#include <iostream>
#include <unordered_map>
#include <utility>
#include <map>
#include <string>
#include <vector>
#include "common.h"

const char TRACE_HASHES_BUCKET_PREFIX[] = "tracehashes";
const char ASTERISK_SERVICE[] = "NONE";

using ::google::cloud::StatusOr;
namespace bg = boost::graph;

class traces_by_structure {
    public: // NOLINT - I don't understand why lint is complaining
        // values
        std::vector<std::string> trace_ids;
        std::vector<std::string> object_names;
        std::vector<std::unordered_map<int, int>> iso_maps;
        std::vector<std::unordered_map<int, std::string>> trace_node_names;
        // maps
        std::map<int, int> iso_map_to_trace_node_names;
        std::map<int, std::vector<int>> object_name_to_trace_ids_of_interest;
        std::map<std::string, std::vector<int>> trace_id_to_isomap_indices;
};

struct trace_structure {
    uint64_t num_nodes;
    std::unordered_map<int, std::string> node_names;
    std::multimap<int, int> edges;
};

struct potential_prefix_struct {
    std::string prefix;
    std::string batch_name;
    std::string trace_id;
};

// This is the highest level function
StatusOr<traces_by_structure> get_traces_by_structure(
    trace_structure query_trace, int start_time, int end_time, gcs::Client* client);

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

        auto prop1 = split_by_string(get(m_property_map1, item1), colon)[0];
        auto prop2 = split_by_string(get(m_property_map2, item2), colon)[0];

        return prop1 == prop2;
    }

    private:  // NOLINT
        const PropertyMapFirst m_property_map1;
        const PropertyMapSecond m_property_map2;
};



// Binary function object that returns true if the values for item1
// in property_map1 and item2 in property_map2 are equivalent.
template < typename PropertyMapFirst, typename PropertyMapSecond >
property_map_equivalent_custom< PropertyMapFirst, PropertyMapSecond > make_property_map_equivalent_custom(
    const PropertyMapFirst property_map1, const PropertyMapSecond property_map2
) {
    return (property_map_equivalent_custom< PropertyMapFirst, PropertyMapSecond >(
        property_map1, property_map2));
}

typedef boost::property<boost::vertex_name_t, std::string, boost::property<boost::vertex_index_t, int> >
vertex_property;
typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS, vertex_property>
graph_type;
typedef boost::property_map<graph_type, boost::vertex_name_t>::type vertex_name_map_t;
typedef property_map_equivalent_custom<vertex_name_map_t, vertex_name_map_t> vertex_comp_t;

/**
 * TODO(haseeb) maybe pass the pointer to IsomorphismMaps, that should be a better practice.
 */
template < typename Graph1, typename Graph2, typename IsomorphismMaps>
struct vf2_callback_custom {
    vf2_callback_custom(const Graph1& graph1, const Graph2& graph2, IsomorphismMaps& isomorphism_maps)
    : graph1_(graph1), graph2_(graph2), isomorphism_maps_(isomorphism_maps) {
    }

    template < typename CorrespondenceMap1To2, typename CorrespondenceMap2To1 >
    bool operator()(CorrespondenceMap1To2 f, CorrespondenceMap2To1) const {
        std::unordered_map<int, int> iso_map;

        BGL_FORALL_VERTICES_T(v, graph1_, Graph1)
        iso_map.insert(
            std::make_pair(
                get(boost::vertex_index_t(), graph1_, v),
                get(boost::vertex_index_t(), graph2_, get(f, v))));

        isomorphism_maps_.push_back(iso_map);
        return true;
    }

    private: // NOLINT
        const Graph1& graph1_;
        const Graph2& graph2_;
        IsomorphismMaps& isomorphism_maps_;
};

std::string get_root_service_name(const std::string &trace);
StatusOr<std::vector<std::string>> filter_trace_ids_based_on_query_timestamp_for_given_root_service(
    std::vector<std::string> &trace_ids,
    std::string &batch_name,
    int start_time,
    int end_time,
    std::string &root_service_name,
    gcs::Client* client);
std::vector<std::unordered_map<int, int>> get_isomorphism_mappings(
    trace_structure &candidate_trace, trace_structure &query_trace);
StatusOr<traces_by_structure> process_trace_hashes_prefix_and_retrieve_relevant_trace_ids(
    std::string prefix, trace_structure query_trace, int start_time, int end_time,
    const std::vector<std::string>& all_object_names, gcs::Client* client);
trace_structure morph_trace_object_to_trace_structure(std::string &trace);
graph_type morph_trace_structure_to_boost_graph_type(trace_structure &input_graph);
StatusOr<std::vector<std::string>> get_trace_ids_from_trace_hashes_object(
    const std::string &object_name, gcs::Client* client);
StatusOr<std::string> get_single_trace_id_from_trace_hashes_object(
    const std::string &object_name, gcs::Client* client);
void print_trace_structure(trace_structure trace);
StatusOr<potential_prefix_struct> get_potential_prefixes(
    std::string prefix, gcs::Client* client);
void merge_traces_by_struct(const traces_by_structure &new_trace_by_struct, traces_by_structure* old);
StatusOr<traces_by_structure> filter_by_query(std::string batch_name,
    std::vector<std::pair<std::string, std::string>> prefix_to_trace_ids,
    trace_structure query_trace, int start_time, int end_time,
    const std::vector<std::string>& all_object_names, gcs::Client* client);
#endif  // BY_STRUCT_H_ // NOLINT
