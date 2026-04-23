#pragma once

#include "taxi/TripRecord.hpp"
#include "taxi/QueryTypes.hpp"
#include "taxi/TimeIndex.hpp"
#include <vector>

namespace taxi {

class QueryEngine {
public:
    explicit QueryEngine(const std::vector<TripRecord>& data);

    // Build indexes (call after data is fully loaded).
    // Returns build time in milliseconds.
    double build_indexes();

    // ---- Single-field range searches (Queries 1-4) ----
    QueryResult search_by_time(const TimeRangeQuery& q) const;
    QueryResult search_by_distance(const NumericRangeQuery& q) const;
    QueryResult search_by_fare(const NumericRangeQuery& q) const;
    QueryResult search_by_location(const IntRangeQuery& q) const;

    // ---- Multi-predicate search (Query 5) ----
    QueryResult search_combined(const CombinedQuery& q) const;

    // ---- Aggregation (Query 6) ----
    AggregationResult aggregate_fare_by_time(const TimeRangeQuery& q) const;

    // ---- Accessors ----
    bool indexes_built() const { return time_index_.is_built(); }

private:
    const std::vector<TripRecord>& data_;
    TimeIndex time_index_;
};

} // namespace taxi
