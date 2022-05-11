#include <gtest/gtest.h>
#include "id_index.h"
#include "bloom_filter.hpp"

// Demonstrate some basic assertions.
TEST(Prefixes, TestGeneratePrefixesSmall) {
  // Expect two strings not to be equal.
  time_t begin = 1651696900;
  time_t end = 1651697000;
  std::vector<std::string> prefixes = generate_prefixes(begin, end);
  std::vector<std::string> expect;
  expect.push_back("16516");
  expect.push_back("16517");
  EXPECT_TRUE(std::find(prefixes.begin(), prefixes.end(), expect[0])!=prefixes.end());
  EXPECT_TRUE(std::find(prefixes.begin(), prefixes.end(), expect[1])!=prefixes.end());
}

TEST(Prefixes, TestGeneratePrefixesBig) {
  // Expect two strings not to be equal.
  time_t begin = 1651691111;
  time_t end = 1651697000;
  std::vector<std::string> prefixes = generate_prefixes(begin, end);
  std::vector<std::string> expect;
  for (int i=1; i<7; i++) {
    expect.push_back("1651" + std::to_string(i));
  }
  for (int i=0; i<expect.size(); i++) {
    EXPECT_TRUE(std::find(prefixes.begin(), prefixes.end(), expect[i])!=prefixes.end());
  }
}

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
