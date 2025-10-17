from typing import Iterable
from ...domain.entities.kqi_raw_data import KQIRawData


def load_kqi_from_rows(rows: Iterable[dict]) -> Iterable[KQIRawData]:
    for r in rows:
        ts = r.get('timestamp') or r.get('time') or ''
        cell = r.get('cell') or r.get('CellID') or ''
        metrics = {k: v for k, v in r.items() if k not in ('timestamp', 'time', 'cell', 'CellID')}
        yield KQIRawData(timestamp=ts, cell=cell, metrics=metrics)

