#pragma once

#include <cstdint>

namespace taxi {

/**
 * @brief Represents a single yellow taxi trip record.
 *
 * All fields use primitive types (int, double, float) for type-correct
 * data structures.
 */
struct TripRecord {
  // Largest-to-smallest ordering to reduce padding.
  std::int64_t
      pickup_timestamp; // tpep_pickup_datetime (seconds since epoch UTC)
  std::int64_t
      dropoff_timestamp; // tpep_dropoff_datetime (seconds since epoch UTC)
  double fare_amount;    // fare_amount
  double tip_amount;     // tip_amount
  double tolls_amount;   // tolls_amount
  double trip_distance;  // trip_distance (miles)
  double total_amount;   // total_amount
  float extra;           // extra
  float mta_tax;         // mta_tax
  float improvement_surcharge; // improvement_surcharge
  std::int16_t pu_location_id; // PULocationID (Taxi Zone ID)
  std::int16_t do_location_id; // DOLocationID (Taxi Zone ID)
  std::uint8_t
      vendor_id; // VendorID (1=Creative Mobile Technologies, 2=VeriFone Inc.)
  std::uint8_t passenger_count;    // passenger_count
  std::uint8_t rate_code_id;       // RatecodeID
  std::uint8_t payment_type;       // payment_type (1=Credit card, 2=Cash, etc.)
  std::uint8_t store_and_fwd_flag; // store_and_fwd_flag (Y/N -> 1/0)

  // Default constructor
  TripRecord() = default;

  // Helper function to check if record is valid (for error handling)
  bool is_valid() const;
};

} // namespace taxi
