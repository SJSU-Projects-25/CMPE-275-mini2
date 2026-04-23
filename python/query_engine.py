from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Literal, Sequence

try:
    from .trip_record import TripRecord
except ImportError:  # pragma: no cover - fallback for script execution
    from trip_record import TripRecord

AggregationOp = Literal["sum", "avg", "count", "all"]


@dataclass(frozen=True)
class QueryExecutionResult:
    records: List[TripRecord]
    aggregation_sum: float
    aggregation_avg: float
    aggregation_count: int


def _compute_aggregation(records: Sequence[TripRecord]) -> tuple[float, float, int]:
    count = len(records)
    if count == 0:
        return 0.0, 0.0, 0
    aggregation_sum = sum(record.fare_amount for record in records)
    aggregation_avg = aggregation_sum / count
    return aggregation_sum, aggregation_avg, count


def _build_result(records: List[TripRecord]) -> QueryExecutionResult:
    aggregation_sum, aggregation_avg, aggregation_count = _compute_aggregation(records)
    return QueryExecutionResult(
        records=records,
        aggregation_sum=aggregation_sum,
        aggregation_avg=aggregation_avg,
        aggregation_count=aggregation_count,
    )


def q1_time_range(
    records: Iterable[TripRecord], start_time: int, end_time: int
) -> QueryExecutionResult:
    matched = [
        record
        for record in records
        if start_time <= record.pickup_timestamp <= end_time
    ]
    return _build_result(matched)


def q2_distance_range(
    records: Iterable[TripRecord], min_val: float, max_val: float
) -> QueryExecutionResult:
    matched = [
        record for record in records if min_val <= record.trip_distance <= max_val
    ]
    return _build_result(matched)


def q3_fare_range(
    records: Iterable[TripRecord], min_val: float, max_val: float
) -> QueryExecutionResult:
    # Keep aligned with C++ QueryEngine::search_by_fare (filters on total_amount).
    matched = [record for record in records if min_val <= record.total_amount <= max_val]
    return _build_result(matched)


def q4_location_range(
    records: Iterable[TripRecord], min_val: int, max_val: int
) -> QueryExecutionResult:
    # Keep aligned with C++ QueryEngine::search_by_location (pickup location only).
    matched = [record for record in records if min_val <= record.pu_location_id <= max_val]
    return _build_result(matched)


def q5_combined(
    records: Iterable[TripRecord],
    start_time: int,
    end_time: int,
    min_distance: float,
    max_distance: float,
    min_passengers: int,
    max_passengers: int,
) -> QueryExecutionResult:
    matched = [
        record
        for record in records
        if (
            start_time <= record.pickup_timestamp <= end_time
            and min_distance <= record.trip_distance <= max_distance
            and min_passengers <= record.passenger_count <= max_passengers
        )
    ]
    return _build_result(matched)


def q6_aggregation(
    records: Iterable[TripRecord],
    start_time: int,
    end_time: int,
    operation: AggregationOp = "all",
) -> QueryExecutionResult:
    matched = [
        record
        for record in records
        if start_time <= record.pickup_timestamp <= end_time
    ]
    base_result = _build_result(matched)

    if operation == "all":
        return base_result
    if operation == "sum":
        return QueryExecutionResult(
            records=base_result.records,
            aggregation_sum=base_result.aggregation_sum,
            aggregation_avg=0.0,
            aggregation_count=0,
        )
    if operation == "avg":
        return QueryExecutionResult(
            records=base_result.records,
            aggregation_sum=0.0,
            aggregation_avg=base_result.aggregation_avg,
            aggregation_count=0,
        )
    if operation == "count":
        return QueryExecutionResult(
            records=base_result.records,
            aggregation_sum=0.0,
            aggregation_avg=0.0,
            aggregation_count=base_result.aggregation_count,
        )
    raise ValueError(f"Unsupported aggregation operation: {operation}")


class QueryEngine:
    def __init__(self, records: Iterable[TripRecord]) -> None:
        self.records: List[TripRecord] = list(records)

    def search_by_time(self, start_time: int, end_time: int) -> QueryExecutionResult:
        return q1_time_range(self.records, start_time, end_time)

    def search_by_distance(self, min_val: float, max_val: float) -> QueryExecutionResult:
        return q2_distance_range(self.records, min_val, max_val)

    def search_by_fare(self, min_val: float, max_val: float) -> QueryExecutionResult:
        return q3_fare_range(self.records, min_val, max_val)

    def search_by_location(self, min_val: int, max_val: int) -> QueryExecutionResult:
        return q4_location_range(self.records, min_val, max_val)

    def search_combined(
        self,
        start_time: int,
        end_time: int,
        min_distance: float,
        max_distance: float,
        min_passengers: int,
        max_passengers: int,
    ) -> QueryExecutionResult:
        return q5_combined(
            self.records,
            start_time,
            end_time,
            min_distance,
            max_distance,
            min_passengers,
            max_passengers,
        )

    def aggregate_fare_by_time(
        self, start_time: int, end_time: int, operation: AggregationOp = "all"
    ) -> QueryExecutionResult:
        return q6_aggregation(self.records, start_time, end_time, operation)

    def execute_request(self, query_request: object) -> QueryExecutionResult:
        query_type = getattr(query_request, "query_type", "")
        if query_type == "time":
            q = getattr(query_request, "time_query")
            return self.search_by_time(q.start_time, q.end_time)
        if query_type == "distance":
            q = getattr(query_request, "numeric_query")
            return self.search_by_distance(q.min_val, q.max_val)
        if query_type == "fare":
            q = getattr(query_request, "numeric_query")
            return self.search_by_fare(q.min_val, q.max_val)
        if query_type == "location":
            q = getattr(query_request, "int_query")
            return self.search_by_location(q.min_val, q.max_val)
        if query_type == "combined":
            q = getattr(query_request, "combined_query")
            return self.search_combined(
                q.time_range.start_time,
                q.time_range.end_time,
                q.distance_range.min_val,
                q.distance_range.max_val,
                q.passenger_range.min_val,
                q.passenger_range.max_val,
            )
        if query_type == "aggregate":
            q = getattr(query_request, "time_query")
            return self.aggregate_fare_by_time(q.start_time, q.end_time, "all")
        raise ValueError(f"Unsupported query_type: {query_type!r}")
