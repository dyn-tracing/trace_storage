#ifndef BY_STRUCT_H_                                                          
#define BY_STRUCT_H_                                                          
                                                                                
#include <iostream>                                                             
#include <unordered_map>                                                        
#include <utility>                                                              
#include <map>                                                                  
#include <string>                                                               
#include <vector>                                                               
#include <future>                                                               
#include "google/cloud/storage/client.h"                                        
#include "opentelemetry/proto/trace/v1/trace.pb.h"                              
#include <boost/algorithm/string.hpp>                                           
#include <boost/graph/adjacency_list.hpp>                                       
#include <boost/graph/vf2_sub_graph_iso.hpp>                                    
#include <boost/date_time/posix_time/posix_time.hpp>                            
#include "common.h"

const int element_count = 10000;

typedef std::tuple<std::string, std::vector<std::string>> objname_to_matching_trace_ids;
typedef std::tuple<std::string, std::vector<int>> trace_id_to_mappings;


// TODO(jessica) trace struct and hashes should be keywords followed by SERVICES_BUCKETS_SUFFIX
const char TRACE_STRUCT_BUCKET[] = "dyntraces-snicket4";
const char TRACE_HASHES_BUCKET[] = "tracehashes-snicket4";
const char ASTERISK_SERVICE[] = "NONE";

namespace gcs = ::google::cloud::storage;
using ::google::cloud::StatusOr;
namespace bg = boost::graph;

class traces_by_structure {
    public:
        std::vector<objname_to_matching_trace_ids> obj_to_trace_ids;
        std::vector<std::unordered_map<int, int>> iso_maps;
};

struct trace_structure {
    int num_nodes;
    std::unordered_map<int, std::string> node_names;
    std::multimap<int, int> edges;
};

std::vector<std::string> split_by_char(std::string input, std::string splitter);

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

        auto prop1 = split_by_char(get(m_property_map1, item1), ":")[0];
        auto prop2 = split_by_char(get(m_property_map2, item2), ":")[0];

        return prop1 == prop2;
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

typedef boost::property<boost::vertex_name_t, std::string, boost::property<boost::vertex_index_t, int> >
vertex_property;                                                                
typedef boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS, vertex_property>
graph_type;
typedef boost::property_map<graph_type, boost::vertex_name_t>::type vertex_name_map_t;
typedef property_map_equivalent_custom<vertex_name_map_t, vertex_name_map_t> vertex_comp_t;


// Binary function object that returns true if the values for item1
// in property_map1 and item2 in property_map2 are equivalent.



/**
 * TODO: maybe pass the pointer to IsomorphismMaps, that should be a better practice.
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

    private:
        const Graph1& graph1_;
        const Graph2& graph2_;
        IsomorphismMaps& isomorphism_maps_;
};

std::vector<std::unordered_map<int, int>> get_isomorphism_mappings(
    trace_structure candidate_trace, trace_structure query_trace);
std::map<std::string, std::string> get_trace_id_to_root_service_map(std::string object_content);
std::map<std::string, std::vector<std::string>> get_root_service_to_trace_ids_map(
    std::map<std::string, std::string> trace_id_to_root_service_map);
std::vector<traces_by_structure> get_traces_by_structure(                       
    trace_structure query_trace, int start_time, int end_time, gcs::Client* client);
traces_by_structure process_trace_hashes_prefix_and_retrieve_relevant_trace_ids(
    std::string prefix, trace_structure query_trace, int start_time, int end_time,
    gcs::Client* client);
std::string extract_batch_name(std::string object_name);
std::pair<int, int> extract_batch_timestamps(std::string batch_name);
std::string strip_from_the_end(std::string object, char stripper);
trace_structure morph_trace_object_to_trace_structure(std::string trace);
graph_type morph_trace_structure_to_boost_graph_type(trace_structure input_graph);
std::string strip_from_the_end(std::string object, char stripper);
std::vector<std::string> get_trace_ids_from_trace_hashes_object(std::string object_name, gcs::Client* client);
#endif // BY_STRUCT_H_                                                          
