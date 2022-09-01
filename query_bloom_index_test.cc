#include <gtest/gtest.h>

#include "query_bloom_index.h"

TEST(Index, GetNearestNodes) {
  std::cout << "HELLOOOOOOOO" << std::endl << std::flush;
  std::pair<time_t, time_t> ret = get_nearest_node(
        /* root */ std::make_pair(0, 100),
        /* granularity */ 10,
        /* start time */ 0,
        /* end time */ 10
  );
  std::cout << "HELLO))))" << std::endl;
  EXPECT_EQ(std::get<0>(ret), 0);
  EXPECT_EQ(std::get<1>(ret), 10);
}
