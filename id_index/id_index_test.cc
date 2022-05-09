#include <gtest/gtest.h>
#include "id_index.h"

//#include <id_index_lib>
// Demonstrate some basic assertions.
TEST(Prefixes, TestGeneratePrefixes) {
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

