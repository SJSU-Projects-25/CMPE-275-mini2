# TripRecord Layout Report (Mini2 Phase 0.4)

This report captures `sizeof(TripRecord)` and field offsets after type tightening
and largest-to-smallest field ordering.

## Build/Test Command

```bash
c++ -std=c++20 -Icpp/include cpp/src/inspect_triprecord_layout.cpp -o /tmp/inspect_triprecord_layout
/tmp/inspect_triprecord_layout
```

## Results

- `sizeof(TripRecord) = 80` bytes

| Field | Offset (bytes) |
|---|---:|
| `pickup_timestamp` | 0 |
| `dropoff_timestamp` | 8 |
| `fare_amount` | 16 |
| `tip_amount` | 24 |
| `tolls_amount` | 32 |
| `trip_distance` | 40 |
| `total_amount` | 48 |
| `extra` | 56 |
| `mta_tax` | 60 |
| `improvement_surcharge` | 64 |
| `pu_location_id` | 68 |
| `do_location_id` | 70 |
| `vendor_id` | 72 |
| `passenger_count` | 73 |
| `rate_code_id` | 74 |
| `payment_type` | 75 |
| `store_and_fwd_flag` | 76 |

## Verification Notes

- Compared to a wider primitive layout from Mini1, this reduced record size and
  packs more records per cache line.
- CSV parsing remained valid after type changes:

```bash
c++ -std=c++20 -Icpp/include cpp/src/TripRecord.cpp cpp/src/CsvReader.cpp cpp/src/verify_csv_load.cpp -o /tmp/verify_csv_load
/tmp/verify_csv_load data/test_sample.csv
# records=9967 rows_read=10000 parsed_ok=9967 discarded=33
```
