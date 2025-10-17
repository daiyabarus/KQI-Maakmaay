from abc import ABC, abstractmethod
from typing import Iterable
from ..entities.kqi_raw_data import KQIRawData


class KQIRepository(ABC):
    """Abstract interface for KQI-specific data access."""

    @abstractmethod
    def stream_kqi(self) -> Iterable[KQIRawData]:
        raise NotImplementedError

