from abc import ABC, abstractmethod
from typing import Iterable
from ..entities.source_mapping import SourceMapping


class MappingRepository(ABC):
    """Abstract interface for loading source field mappings."""

    @abstractmethod
    def load_mappings(self) -> Iterable[SourceMapping]:
        raise NotImplementedError

