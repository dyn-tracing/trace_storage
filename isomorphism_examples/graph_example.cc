#include <boost/graph/adjacency_list.hpp>
#include <boost/graph/vf2_sub_graph_iso.hpp>
using namespace boost;
// https://github.com/hsnks100/booang/blob/c027e130c69e4176d24d2199cdf48282c6518fca/example/vf2_sub_graph_iso_example.cpp

int main() {

  typedef adjacency_list<setS, vecS, bidirectionalS> graph_type;
  
  // Build graph1
  int num_vertices1 = 2; 
  graph_type graph1(num_vertices1);
  add_edge(0, 1, graph1);

  // Build graph2
  int num_vertices2 = 3; 
  graph_type graph2(num_vertices2);
  add_edge(0, 1, graph2);
  add_edge(0, 2, graph2);
  add_edge(1, 2, graph2);
  add_edge(2, 1, graph2);

  // Create callback to print mappings
  vf2_print_callback<graph_type, graph_type> callback(graph1, graph2);

  // Print out all subgraph isomorphism mappings between graph1 and graph2.
  // Vertices and edges are assumed to be always equivalent.
  vf2_subgraph_iso(graph1, graph2, callback);

  return 0;
}