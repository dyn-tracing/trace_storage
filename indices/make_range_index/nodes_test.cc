#include <gtest/gtest.h>
#include "nodes.h"

TEST(Serialization, TestSerializeNode) {
    Node node;
    node.start_time = 5;
    node.end_time = 10;
    node.data.push_back(IndexedData {
        .batch_name = "12-34-56",
        .trace_id = "12345",
        .data = "abc"
    });
    node.data.push_back(IndexedData {
        .batch_name = "12-34-59",
        .trace_id = "12334",
        .data = "def"
    });

    std::stringstream stream;
    node.Serialize(stream);

    Node node2;
    node2.Deserialize(stream);

    EXPECT_EQ(node2, node);
}

TEST(Serialization, TestSerializeNodeSummary) {
    NodeSummary ns;
    ns.start_time = 1;
    ns.end_time = 10;
    ns.node_objects.push_back(std::make_pair(2, "abc"));
    ns.node_objects.push_back(std::make_pair(3, "def"));

    std::stringstream stream;
    ns.Serialize(stream);

    NodeSummary ns2;
    ns2.Deserialize(stream);
    EXPECT_EQ(ns2, ns);
}
