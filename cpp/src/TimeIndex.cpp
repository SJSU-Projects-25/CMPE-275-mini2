#include "taxi/TimeIndex.hpp"
#include <algorithm>
#include <numeric>

namespace taxi {

void TimeIndex::build(const std::vector<TripRecord>& records) {
    const std::size_t n = records.size();
    indices_.resize(n);

    std::iota(indices_.begin(), indices_.end(), std::size_t{0});

    std::sort(indices_.begin(), indices_.end(),
        [&records](std::size_t a, std::size_t b) {
            return records[a].pickup_timestamp < records[b].pickup_timestamp;
        }
    );

    built_ = true;
}

std::pair<std::size_t, std::size_t> TimeIndex::lookup(
    const std::vector<TripRecord>& records,
    std::int64_t start_time,
    std::int64_t end_time
) const {
    if (!built_ || indices_.empty()) {
        return {0, 0};
    }

    auto lo = std::lower_bound(
        indices_.begin(), indices_.end(), start_time,
        [&records](std::size_t idx, std::int64_t val) {
            return records[idx].pickup_timestamp < val;
        }
    );

    auto hi = std::upper_bound(
        lo, indices_.end(), end_time,
        [&records](std::int64_t val, std::size_t idx) {
            return val < records[idx].pickup_timestamp;
        }
    );

    return {
        static_cast<std::size_t>(std::distance(indices_.begin(), lo)),
        static_cast<std::size_t>(std::distance(indices_.begin(), hi))
    };
}

} // namespace taxi
