#include "taxi/TripRecord.hpp"

namespace taxi {

bool TripRecord::is_valid() const {
    // Basic validation: ensure timestamps are reasonable and positive amounts
    return pickup_timestamp > 0 && 
           dropoff_timestamp > pickup_timestamp &&
           passenger_count >= 0 &&
           trip_distance >= 0.0 &&
           total_amount >= 0.0;
}

} // namespace taxi
