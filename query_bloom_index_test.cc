#include <gtest/gtest.h>

#include "query_bloom_index.h"

TEST(Index, GetNearestNodeForLeaf) {
  std::pair<time_t, time_t> ret = get_nearest_node(
        /* root */ std::make_pair(0, 100),
        /* granularity */ 10,
        /* start time */ 0,
        /* end time */ 10
  );
  EXPECT_EQ(std::get<0>(ret), 0);
  EXPECT_EQ(std::get<1>(ret), 10);
}

TEST(Index, GetNearestNodesRoot) {
  std::pair<time_t, time_t> ret = get_nearest_node(
        /* root */ std::make_pair(0, 100),
        /* granularity */ 10,
        /* start time */ 0,
        /* end time */ 90
  );
  EXPECT_EQ(std::get<0>(ret), 0);
  EXPECT_EQ(std::get<1>(ret), 100);
}

TEST(Index, GetNearestNodesMid) {
  std::pair<time_t, time_t> ret = get_nearest_node(
        /* root */ std::make_pair(0, 1000),
        /* granularity */ 10,
        /* start time */ 10,
        /* end time */ 90
  );
  EXPECT_EQ(std::get<0>(ret), 0);
  EXPECT_EQ(std::get<1>(ret), 100);
}
