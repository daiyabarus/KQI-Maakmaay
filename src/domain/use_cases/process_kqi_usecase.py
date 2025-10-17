from typing import Iterable
from ..entities.kqi_raw_data import KQIRawData


def process_kqi(records: Iterable[KQIRawData]):
    """Lightweight processing pipeline for KQI records.

    Currently yields the records unchanged; meant to be extended.
    """
    for r in records:
        yield r

