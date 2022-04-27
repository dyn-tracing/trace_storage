#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/vf2_sub_graph_iso.hpp>
using namespace boost;
// https://github.com/hsnks100/booang/blob/c027e130c69e4176d24d2199cdf48282c6518fca/example/vf2_sub_graph_iso_example.cpp


int main() {

  typedef property<vertex_name_t, std::string, property<vertex_index_t, int> > vertex_property;

  // Using a vecS graphs => the index maps are implicit.
  typedef adjacency_list<vecS, vecS, bidirectionalS, vertex_property> graph_type;
  
  
  // Build graph1
  graph_type graph1(2);
  // add_vertex(vertex_property("a", 0), graph1);
  // add_vertex(vertex_property("b", 1), graph1);
  add_edge(0, 1, graph1);

  // Build graph2
  graph_type graph2(3);
  // add_vertex(vertex_property("a", 0), graph2);
  // add_vertex(vertex_property("b", 1), graph2);
  // add_vertex(vertex_property("c", 2), graph2);
  add_edge(0, 1, graph2);
  add_edge(0, 2, graph2);
  add_edge(1, 2, graph2);
  add_edge(2, 1, graph2);


  // create predicates
  typedef property_map<graph_type, vertex_name_t>::type vertex_name_map_t;
  typedef property_map_equivalent<vertex_name_map_t, vertex_name_map_t> vertex_comp_t;
  vertex_comp_t vertex_comp =
    make_property_map_equivalent(get(vertex_name, graph1), get(vertex_name, graph2));
  
  // typedef property_map<graph_type, edge_name_t>::type edge_name_map_t;
  // typedef property_map_equivalent<edge_name_map_t, edge_name_map_t> edge_comp_t;
  // edge_comp_t edge_comp =
  //   make_property_map_equivalent(get(edge_name, graph1), get(edge_name, graph2));
 
  // Create callback to print mappings
  vf2_print_callback<graph_type, graph_type> callback(graph1, graph2);

  // Print out all subgraph isomorphism mappings between graph1 and graph2.
  // Vertices and edges are assumed to be always equivalent.
  bool res = vf2_subgraph_iso(graph1, graph2, callback, vertex_order_by_mult(graph1), vertices_equivalent(vertex_comp));
  std::cout << "Response: " << res << std::endl;

  return 0;
}