from typing import Iterable
from ...domain.repositories.kqi_repository import KQIRepository
from ...domain.entities.kqi_raw_data import KQIRawData


class KQIRepositoryImpl(KQIRepository):
    def __init__(self, rows_iterable: Iterable[dict]):
        self._rows = rows_iterable

    def stream_kqi(self) -> Iterable[KQIRawData]:
        for r in self._rows:
            ts = r.get('timestamp') or r.get('time') or ''
            cell = r.get('cell') or r.get('CellID') or ''
            metrics = {k: v for k, v in r.items() if k not in ('timestamp', 'time', 'cell', 'CellID')}
            yield KQIRawData(timestamp=ts, cell=cell, metrics=metrics)

