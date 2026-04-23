#pragma once

#include "taxi/TripRecord.hpp"
#include <cstdint>
#include <cstddef>
#include <vector>

namespace taxi {

// ---- Query parameter structs ----

struct TimeRangeQuery {
    std::int64_t start_time;   // epoch seconds, inclusive
    std::int64_t end_time;     // epoch seconds, inclusive
};

struct NumericRangeQuery {
    double min_val;
    double max_val;
};

struct IntRangeQuery {
    int min_val;
    int max_val;
};

struct CombinedQuery {
    TimeRangeQuery   time_range;
    NumericRangeQuery distance_range;
    IntRangeQuery    passenger_range;
};

// ---- Result types ----

struct QueryResult {
    std::vector<const TripRecord*> records;
    std::size_t scanned = 0;   // how many records were examined
};

struct AggregationResult {
    double sum   = 0.0;
    double avg   = 0.0;
    std::size_t count = 0;
};

} // namespace taxi
