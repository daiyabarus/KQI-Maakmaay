from typing import Iterable, Mapping
from ...domain.repositories.csv_repository import CSVRepository


class CSVRepositoryImpl(CSVRepository):
    def __init__(self, file_list):
        self._files = list(file_list)

    def list_files(self, path: str) -> Iterable[str]:
        # For simplicity, ignore path and return provided files
        return iter(self._files)

    def read_rows(self, file_path: str) -> Iterable[Mapping[str, str]]:
        # Basic implementation expects a callable or path; keep flexible
        if callable(file_path):
            return file_path()
        # else assume it's an iterable of rows saved on init
        return []

