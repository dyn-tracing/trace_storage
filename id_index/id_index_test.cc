#include <gtest/gtest.h>
#include "id_index.h"
#include "bloom_filter.hpp"

TEST(Prefixes, TestGeneratePrefixesSmall) {
  // Expect two strings not to be equal.
  time_t begin = 1651696900;
  time_t end = 1651697000;
  std::vector<std::string> prefixes = generate_prefixes(begin, end);
  std::vector<std::string> expect;
  expect.push_back("1651696");
  expect.push_back("1651697");
  for (int i=0; i < prefixes.size(); i++) {
    EXPECT_EQ(prefixes[i].compare(expect[i]), 0);
  }
}


TEST(Prefixes, TestGeneratePrefixesBig) {
  // Expect two strings not to be equal.
  time_t begin = 1651691111;
  time_t end = 1651697000;
  std::vector<std::string> prefixes = generate_prefixes(begin, end);
  std::vector<std::string> expect;
  for (int i=1; i <= 7; i++) {
    expect.push_back("165169" + std::to_string(i));
  }
  for (int i=0; i < expect.size(); i++) {
    EXPECT_EQ(prefixes[i].compare(expect[i]), 0);
  }
}

TEST(Serialization, TestSerializationBloom) {
  // Expect two strings not to be equal.
  bloom_parameters a_param;
  a_param.projected_element_count = 100;
  a_param.false_positive_probability = 0.01;
  a_param.compute_optimal_parameters();
  bloom_filter a(a_param);
  a.insert("123");
  std::stringstream stream;
  a.Serialize(stream);
  bloom_filter b;
  b.Deserialize(stream);
  EXPECT_TRUE(a.ints_match(b));
  EXPECT_TRUE(a.salt_matches(b));
  EXPECT_TRUE(a.bit_table_matches(b));
  EXPECT_EQ(a, b);
}

TEST(Serialization, TestSerializationLeaf) {
  Leaf leaf1, leaf2;
  for (int i=0; i < 20; i++) {
    leaf1.batch_names.push_back("name"+std::to_string(i));
    bloom_parameters a_param;
      a_param.projected_element_count = 100;
      a_param.false_positive_probability = 0.01;
      a_param.compute_optimal_parameters();
      bloom_filter a(a_param);
      a.insert("abc"+std::to_string(i));
      leaf1.bloom_filters.push_back(a);
  }
  std::stringstream stream;
  serialize_leaf(leaf1, stream);
  leaf2 = deserialize_leaf(stream);
  EXPECT_TRUE(leaf_sizes_equal(leaf1, leaf2));
  EXPECT_TRUE(batch_names_equal(leaf1, leaf2));
  EXPECT_EQ(leaf1.bloom_filters.size(), leaf2.bloom_filters.size());
  for (int i=0; i < leaf1.bloom_filters.size(); i++) {
    EXPECT_TRUE(leaf1.bloom_filters[i].ints_match(leaf2.bloom_filters[i]));
  }
  EXPECT_TRUE(bloom_filters_equal(leaf1, leaf2));
  for (int i=0; i < 20; i++) {
    EXPECT_TRUE(leaf2.bloom_filters[i].contains("abc"+std::to_string(i)));
  }
}

TEST(Arithmetic, TestGetParentArithmetic) {
    auto ret = get_parent(40, 50, 10);
    std::tuple<time_t, time_t> answer = std::make_tuple(0, 100);
    EXPECT_EQ(ret, answer);
    auto ret2 = get_parent(140, 150, 10);
    std::tuple<time_t, time_t> answer2 = std::make_tuple(100, 200);
    EXPECT_EQ(ret2, answer2);
    auto ret3 = get_parent(200, 300, 10);
    std::tuple<time_t, time_t> answer3 = std::make_tuple(0, 1000);
    EXPECT_EQ(ret3, answer3);
}
