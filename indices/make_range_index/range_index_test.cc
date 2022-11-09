#include <gtest/gtest.h>
#include "range_index.h"

TEST(Serialization, TestSerializeNode) {
    Node node;
    node.start_time = 5;
    node.end_time = 10;
    node.start_value = 3;
    node.end_value = 25;
    node.values.push_back(3);
    node.values.push_back(4);
    node.values.push_back(5);
    node.values.push_back(25);

    std::stringstream stream;
    Serialize(node, stream);

    Node node2 = DeserializeNode(stream);

    EXPECT_EQ(node2, node);
}

TEST(Serialization, TestSerializeNodeSummary) {
    NodeSummary ns;
    ns.start_time = 1;
    ns.end_time = 10;
    ns.node_objects.push_back(3);
    ns.node_objects.push_back(29);
    ns.node_objects.push_back(54);

    std::stringstream stream;
    Serialize(ns, stream);

    NodeSummary ns2 = DeserializeNodeSummary(stream);
    EXPECT_EQ(ns2, ns);
}
