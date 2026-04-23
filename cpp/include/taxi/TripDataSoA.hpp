#pragma once

#include "taxi/TripRecord.hpp"
#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>

namespace taxi {

/**
 * @brief Object-of-Arrays (SoA) layout for trip data — Phase 3.
 *
 * Instead of std::vector<TripRecord> (Array-of-Structs / AoS), each field is
 * stored in its own contiguous typed std::vector<T>.
 *
 * Why SoA is faster for scan-heavy workloads:
 *  - Scanning one field (e.g. trip_distance) loads only that field's cache
 *    lines.  In AoS a 128-byte TripRecord struct wastes ~120 bytes of each
 *    64-byte cache line when we only need 8 bytes (one double).
 *  - Contiguous same-typed arrays allow the compiler / hardware to
 *    auto-vectorize (SIMD: SSE/AVX) far more aggressively than interleaved
 *    struct fields.
 *  - Effective cache-line utilisation: 8 doubles vs. 0.5 TripRecords per line.
 */
struct TripDataSoA {
    // ---- parallel arrays — index i corresponds to the i-th trip ----
    std::vector<int>          vendor_id;
    std::vector<std::int64_t> pickup_timestamp;
    std::vector<std::int64_t> dropoff_timestamp;
    std::vector<int>          passenger_count;
    std::vector<double>       trip_distance;
    std::vector<int>          rate_code_id;
    std::vector<std::uint8_t> store_and_fwd_flag;  // uint8_t avoids std::vector<bool> bit-packing, enabling SIMD
    std::vector<int>          pu_location_id;
    std::vector<int>          do_location_id;
    std::vector<int>          payment_type;
    std::vector<double>       fare_amount;
    std::vector<double>       extra;
    std::vector<double>       mta_tax;
    std::vector<double>       tip_amount;
    std::vector<double>       tolls_amount;
    std::vector<double>       improvement_surcharge;
    std::vector<double>       total_amount;

    std::size_t size() const { return pickup_timestamp.size(); }

    /**
     * @brief Convert an AoS vector<TripRecord> to SoA layout.
     *
     * Iterates once over the AoS records and appends each field value to its
     * dedicated vector.  Time O(N), space O(N * total_field_bytes).
     *
     * @param records Source AoS records (unmodified).
     * @return TripDataSoA with all parallel arrays pre-populated.
     */
    static TripDataSoA from_aos(const std::vector<TripRecord>& records);

    /**
     * @brief Load SoA layout directly from CSV files — no intermediate AoS.
     *
     * Single-pass: reads each record via CsvReader and pushes each field
     * directly into its column vector.  Peak memory = SoA only (~N × field_bytes),
     * avoiding the 2× peak of from_aos() (AoS + SoA simultaneously).
     *
     * @param paths          One or more CSV file paths (concatenated in order).
     * @param reserve_count  Pre-reserve this many rows per column (0 = none).
     *                       Pass an overestimate (e.g. 100000000) to avoid
     *                       reallocation during load.
     */
    static TripDataSoA from_csv(const std::vector<std::string>& paths,
                                std::size_t reserve_count = 0);
};

} // namespace taxi
