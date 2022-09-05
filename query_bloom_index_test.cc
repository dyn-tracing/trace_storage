#include <gtest/gtest.h>

#include "query_bloom_index.h"

TEST(Index, GetNearestNodeForLeaf) {
  std::tuple<time_t, time_t> ret = get_nearest_node(
        /* root */ std::make_tuple(0, 100),
        /* granularity */ 10,
        /* start time */ 0,
        /* end time */ 10);
  EXPECT_EQ(std::get<0>(ret), 0);
  EXPECT_EQ(std::get<1>(ret), 10);
}

TEST(Index, GetNearestNodesRoot) {
  std::tuple<time_t, time_t> ret = get_nearest_node(
        /* root */ std::make_tuple(1662220000, 1662230000),
        /* granularity */ 10,
        /* start time */ 1662226680,
        /* end time */ 1662226681);
  EXPECT_EQ(std::get<0>(ret), 0);
  EXPECT_EQ(std::get<1>(ret), 100);
}

TEST(Index, GetNearestNodesMid) {
  std::tuple<time_t, time_t> ret = get_nearest_node(
        /* root */ std::make_tuple(0, 1000),
        /* granularity */ 10,
        /* start time */ 10,
        /* end time */ 90);
  EXPECT_EQ(std::get<0>(ret), 0);
  EXPECT_EQ(std::get<1>(ret), 100);
}
