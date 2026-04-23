#include "taxi/TripRecord.hpp"

#include <cstddef>
#include <iostream>

int main() {
    using taxi::TripRecord;

    std::cout << "sizeof(TripRecord)=" << sizeof(TripRecord) << "\n";
    std::cout << "offsetof(pickup_timestamp)=" << offsetof(TripRecord, pickup_timestamp) << "\n";
    std::cout << "offsetof(dropoff_timestamp)=" << offsetof(TripRecord, dropoff_timestamp) << "\n";
    std::cout << "offsetof(fare_amount)=" << offsetof(TripRecord, fare_amount) << "\n";
    std::cout << "offsetof(tip_amount)=" << offsetof(TripRecord, tip_amount) << "\n";
    std::cout << "offsetof(tolls_amount)=" << offsetof(TripRecord, tolls_amount) << "\n";
    std::cout << "offsetof(trip_distance)=" << offsetof(TripRecord, trip_distance) << "\n";
    std::cout << "offsetof(total_amount)=" << offsetof(TripRecord, total_amount) << "\n";
    std::cout << "offsetof(extra)=" << offsetof(TripRecord, extra) << "\n";
    std::cout << "offsetof(mta_tax)=" << offsetof(TripRecord, mta_tax) << "\n";
    std::cout << "offsetof(improvement_surcharge)=" << offsetof(TripRecord, improvement_surcharge) << "\n";
    std::cout << "offsetof(pu_location_id)=" << offsetof(TripRecord, pu_location_id) << "\n";
    std::cout << "offsetof(do_location_id)=" << offsetof(TripRecord, do_location_id) << "\n";
    std::cout << "offsetof(vendor_id)=" << offsetof(TripRecord, vendor_id) << "\n";
    std::cout << "offsetof(passenger_count)=" << offsetof(TripRecord, passenger_count) << "\n";
    std::cout << "offsetof(rate_code_id)=" << offsetof(TripRecord, rate_code_id) << "\n";
    std::cout << "offsetof(payment_type)=" << offsetof(TripRecord, payment_type) << "\n";
    std::cout << "offsetof(store_and_fwd_flag)=" << offsetof(TripRecord, store_and_fwd_flag) << "\n";
    return 0;
}
