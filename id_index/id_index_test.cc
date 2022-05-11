#include <gtest/gtest.h>
#include "id_index.h"
#include "bloom_filter.hpp"



TEST(HelloTest, BasicAssertions) {
  // Expect two strings not to be equal.
  EXPECT_STRNE("hello", "world");
  // Expect equality.
  EXPECT_EQ(7 * 6, 42);
}

TEST(Prefixes, TestGeneratePrefixesSmall) {
  // Expect two strings not to be equal.
  time_t begin = 1651696900;
  time_t end = 1651697000;
  std::vector<std::string> prefixes = generate_prefixes(begin, end);
  std::vector<std::string> expect;
  expect.push_back("1651696");
  expect.push_back("1651697");
  for (int i=0; i<prefixes.size(); i++ ) {
    EXPECT_TRUE(prefixes[i].compare(expect[i])==0);
  }
}


TEST(Prefixes, TestGeneratePrefixesBig) {
  // Expect two strings not to be equal.
  time_t begin = 1651691111;
  time_t end = 1651697000;
  std::vector<std::string> prefixes = generate_prefixes(begin, end);
  std::vector<std::string> expect;
  for (int i=1; i<=7; i++) {
    expect.push_back("165169" + std::to_string(i));
  }
  for (int i=0; i<expect.size(); i++) {
    EXPECT_TRUE(prefixes[i].compare(expect[i])==0);
  }
}

/*
TEST(Serialization, TestSerialization) {
  // Expect two strings not to be equal.
  bloom_parameters a_param;
  a_param.projected_element_count = 100;
  a_param.false_positive_probability = 0.01;
  a_param.compute_optimal_parameters();
  bloom_filter a(a_param);
  std::stringstream stream;
  a.Serialize(stream);
  bloom_filter b;
  b.Deserialize(stream);
  EXPECT_TRUE(a==b);
}
*/
