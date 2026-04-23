#include "taxi/QueryEngine.hpp"
#include <chrono>

#if defined(_OPENMP)
#include <omp.h>
#endif

namespace taxi {

QueryEngine::QueryEngine(const std::vector<TripRecord>& data)
    : data_(data) {}

double QueryEngine::build_indexes() {
    auto start = std::chrono::steady_clock::now();
    time_index_.build(data_);
    auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
}

// Query 1: Time range — uses TimeIndex for O(log N) lookup
QueryResult QueryEngine::search_by_time(const TimeRangeQuery& q) const {
    QueryResult result;

    if (time_index_.is_built()) {
        auto [lo_, hi_] = time_index_.lookup(data_, q.start_time, q.end_time);
        const std::size_t lo = lo_, hi = hi_;
        const auto& idx = time_index_.sorted_indices();
        result.scanned = hi - lo;

        if (hi - lo < 10000) {
            result.records.reserve(hi - lo);
            for (std::size_t i = lo; i < hi; ++i) {
                result.records.push_back(&data_[idx[i]]);
            }
        } else {
            #pragma omp parallel
            {
                std::vector<const TripRecord*> local;
#if defined(_OPENMP)
                local.reserve((hi - lo) / omp_get_num_threads());
#else
                local.reserve(hi - lo);
#endif
                #pragma omp for nowait schedule(static)
                for (std::size_t i = lo; i < hi; ++i) {
                    local.push_back(&data_[idx[i]]);
                }

                #pragma omp critical
                {
                    result.records.insert(
                        result.records.end(), local.begin(), local.end());
                }
            }
        }
    } else {
        result.scanned = data_.size();
        #pragma omp parallel
        {
            std::vector<const TripRecord*> local;
#if defined(_OPENMP)
            local.reserve(data_.size() / (10 * omp_get_num_threads()));
#else
            local.reserve(data_.size() / 10);
#endif
            #pragma omp for nowait schedule(static)
            for (std::size_t i = 0; i < data_.size(); ++i) {
                if (data_[i].pickup_timestamp >= q.start_time &&
                    data_[i].pickup_timestamp <= q.end_time) {
                    local.push_back(&data_[i]);
                }
            }

            #pragma omp critical
            {
                result.records.insert(
                    result.records.end(), local.begin(), local.end());
            }
        }
    }

    return result;
}

// Query 2: Distance range — parallel linear scan
QueryResult QueryEngine::search_by_distance(const NumericRangeQuery& q) const {
    QueryResult result;
    result.scanned = data_.size();

    #pragma omp parallel
    {
        std::vector<const TripRecord*> local;
#if defined(_OPENMP)
        local.reserve(data_.size() / (10 * omp_get_num_threads()));
#else
        local.reserve(data_.size() / 10);
#endif
        #pragma omp for nowait schedule(static)
        for (std::size_t i = 0; i < data_.size(); ++i) {
            if (data_[i].trip_distance >= q.min_val &&
                data_[i].trip_distance <= q.max_val) {
                local.push_back(&data_[i]);
            }
        }

        #pragma omp critical
        {
            result.records.insert(
                result.records.end(), local.begin(), local.end());
        }
    }

    return result;
}

// Query 3: Fare range — parallel linear scan
QueryResult QueryEngine::search_by_fare(const NumericRangeQuery& q) const {
    QueryResult result;
    result.scanned = data_.size();

    #pragma omp parallel
    {
        std::vector<const TripRecord*> local;
#if defined(_OPENMP)
        local.reserve(data_.size() / (10 * omp_get_num_threads()));
#else
        local.reserve(data_.size() / 10);
#endif
        #pragma omp for nowait schedule(static)
        for (std::size_t i = 0; i < data_.size(); ++i) {
            if (data_[i].total_amount >= q.min_val &&
                data_[i].total_amount <= q.max_val) {
                local.push_back(&data_[i]);
            }
        }

        #pragma omp critical
        {
            result.records.insert(
                result.records.end(), local.begin(), local.end());
        }
    }

    return result;
}

// Query 4: Location filter — parallel linear scan on PULocationID
QueryResult QueryEngine::search_by_location(const IntRangeQuery& q) const {
    QueryResult result;
    result.scanned = data_.size();

    #pragma omp parallel
    {
        std::vector<const TripRecord*> local;
#if defined(_OPENMP)
        local.reserve(data_.size() / (10 * omp_get_num_threads()));
#else
        local.reserve(data_.size() / 10);
#endif
        #pragma omp for nowait schedule(static)
        for (std::size_t i = 0; i < data_.size(); ++i) {
            if (data_[i].pu_location_id >= q.min_val &&
                data_[i].pu_location_id <= q.max_val) {
                local.push_back(&data_[i]);
            }
        }

        #pragma omp critical
        {
            result.records.insert(
                result.records.end(), local.begin(), local.end());
        }
    }

    return result;
}

// Query 5: Combined — time + distance + passenger count
// Uses TimeIndex to narrow window, then parallel-filters.
QueryResult QueryEngine::search_combined(const CombinedQuery& q) const {
    QueryResult result;

    if (time_index_.is_built()) {
        auto [lo_, hi_] = time_index_.lookup(
            data_, q.time_range.start_time, q.time_range.end_time);
        const std::size_t lo = lo_, hi = hi_;
        const auto& idx = time_index_.sorted_indices();
        result.scanned = hi - lo;

        #pragma omp parallel
        {
            std::vector<const TripRecord*> local;
#if defined(_OPENMP)
            local.reserve((hi - lo) / (10 * omp_get_num_threads()));
#else
            local.reserve((hi - lo) / 10);
#endif
            #pragma omp for nowait schedule(static)
            for (std::size_t i = lo; i < hi; ++i) {
                const auto& rec = data_[idx[i]];
                if (rec.trip_distance >= q.distance_range.min_val &&
                    rec.trip_distance <= q.distance_range.max_val &&
                    rec.passenger_count >= q.passenger_range.min_val &&
                    rec.passenger_count <= q.passenger_range.max_val) {
                    local.push_back(&rec);
                }
            }

            #pragma omp critical
            {
                result.records.insert(
                    result.records.end(), local.begin(), local.end());
            }
        }
    } else {
        result.scanned = data_.size();
        #pragma omp parallel
        {
            std::vector<const TripRecord*> local;
#if defined(_OPENMP)
            local.reserve(data_.size() / (20 * omp_get_num_threads()));
#else
            local.reserve(data_.size() / 20);
#endif
            #pragma omp for nowait schedule(static)
            for (std::size_t i = 0; i < data_.size(); ++i) {
                if (data_[i].pickup_timestamp >= q.time_range.start_time &&
                    data_[i].pickup_timestamp <= q.time_range.end_time &&
                    data_[i].trip_distance >= q.distance_range.min_val &&
                    data_[i].trip_distance <= q.distance_range.max_val &&
                    data_[i].passenger_count >= q.passenger_range.min_val &&
                    data_[i].passenger_count <= q.passenger_range.max_val) {
                    local.push_back(&data_[i]);
                }
            }

            #pragma omp critical
            {
                result.records.insert(
                    result.records.end(), local.begin(), local.end());
            }
        }
    }

    return result;
}

// Query 6: Aggregation — sum/avg of fare_amount over a time window
// Uses reduction for thread-safe accumulation.
AggregationResult QueryEngine::aggregate_fare_by_time(const TimeRangeQuery& q) const {
    AggregationResult result;

    if (time_index_.is_built()) {
        auto [lo_, hi_] = time_index_.lookup(data_, q.start_time, q.end_time);
        const std::size_t lo = lo_, hi = hi_;
        const auto& idx = time_index_.sorted_indices();
        result.count = hi - lo;

        double local_sum = 0.0;
        #pragma omp parallel for reduction(+:local_sum) schedule(static)
        for (std::size_t i = lo; i < hi; ++i) {
            local_sum += data_[idx[i]].fare_amount;
        }
        result.sum = local_sum;
    } else {
        double local_sum = 0.0;
        std::size_t local_count = 0;

        #pragma omp parallel for reduction(+:local_sum,local_count) schedule(static)
        for (std::size_t i = 0; i < data_.size(); ++i) {
            if (data_[i].pickup_timestamp >= q.start_time &&
                data_[i].pickup_timestamp <= q.end_time) {
                local_sum += data_[i].fare_amount;
                ++local_count;
            }
        }
        result.sum = local_sum;
        result.count = local_count;
    }

    if (result.count > 0) {
        result.avg = result.sum / static_cast<double>(result.count);
    }

    return result;
}

} // namespace taxi
