#pragma once

#include <cstdint>

namespace taxi {

/**
 * @brief Represents a single yellow taxi trip record.
 * 
 * All fields use primitive types as required by Phase 1.
 * This structure will be used in Phase 1 (Array-of-Objects) and
 * later optimized in Phase 3 (Object-of-Arrays).
 */
struct TripRecord {
    int vendor_id;                    // VendorID (1=Creative Mobile Technologies, 2=VeriFone Inc.)
    std::int64_t pickup_timestamp;    // tpep_pickup_datetime (seconds since epoch UTC)
    std::int64_t dropoff_timestamp;   // tpep_dropoff_datetime (seconds since epoch UTC)
    int passenger_count;              // passenger_count
    double trip_distance;             // trip_distance (miles)
    int rate_code_id;                 // RatecodeID
    bool store_and_fwd_flag;          // store_and_fwd_flag (Y/N -> true/false)
    int pu_location_id;               // PULocationID (Taxi Zone ID)
    int do_location_id;               // DOLocationID (Taxi Zone ID)
    int payment_type;                 // payment_type (1=Credit card, 2=Cash, etc.)
    double fare_amount;               // fare_amount
    double extra;                     // extra
    double mta_tax;                   // mta_tax
    double tip_amount;                // tip_amount
    double tolls_amount;              // tolls_amount
    double improvement_surcharge;    // improvement_surcharge
    double total_amount;              // total_amount

    // Default constructor
    TripRecord() = default;

    // Helper function to check if record is valid (for error handling)
    bool is_valid() const;
};

} // namespace taxi
