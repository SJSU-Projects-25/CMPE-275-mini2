from dataclasses import dataclass


@dataclass
class TripRecord:
    vendor_id: int
    pickup_timestamp: int
    dropoff_timestamp: int
    passenger_count: int
    trip_distance: float
    rate_code_id: int
    store_and_fwd_flag: bool
    pu_location_id: int
    do_location_id: int
    payment_type: int
    fare_amount: float
    extra: float
    mta_tax: float
    tip_amount: float
    tolls_amount: float
    improvement_surcharge: float
    total_amount: float
