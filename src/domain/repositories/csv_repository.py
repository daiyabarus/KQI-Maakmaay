from abc import ABC, abstractmethod
from typing import Iterable, Mapping


class CSVRepository(ABC):
    """Abstract interface for generic CSV data access."""

    @abstractmethod
    def list_files(self, path: str) -> Iterable[str]:
        raise NotImplementedError

    @abstractmethod
    def read_rows(self, file_path: str) -> Iterable[Mapping[str, str]]:
        raise NotImplementedError

