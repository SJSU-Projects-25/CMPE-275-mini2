#pragma once

#include "taxi/TripRecord.hpp"
#include <vector>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace taxi {

class TimeIndex {
public:
    TimeIndex() = default;

    void build(const std::vector<TripRecord>& records);

    // Return [begin_idx, end_idx) into indices_ for the given time range.
    std::pair<std::size_t, std::size_t> lookup(
        const std::vector<TripRecord>& records,
        std::int64_t start_time,
        std::int64_t end_time
    ) const;

    const std::vector<std::size_t>& sorted_indices() const { return indices_; }
    bool is_built() const { return built_; }
    std::size_t size() const { return indices_.size(); }

private:
    std::vector<std::size_t> indices_;
    bool built_ = false;
};

} // namespace taxi
