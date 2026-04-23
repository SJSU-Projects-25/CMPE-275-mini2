from typing import Iterable, List

from .trip_record import TripRecord


class QueryEngine:
    """
    Placeholder for the Mini 2 Python-side local query execution engine.
    Concrete query methods are added in later phases.
    """

    def __init__(self, records: Iterable[TripRecord]) -> None:
        self.records: List[TripRecord] = list(records)
