#include <gtest/gtest.h>
#include "trace_storage.h"

namespace {
    TEST(test_is_object_within_timespan, positive) {
        EXPECT_FALSE(is_object_within_timespan("12-123-125", 123, 124));
    }
}

