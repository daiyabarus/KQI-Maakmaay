from collections import defaultdict
from typing import Iterable, Dict
from ..entities.kqi_raw_data import KQIRawData


def group_by_mnc(records: Iterable[KQIRawData]) -> Dict[str, list]:
    groups = defaultdict(list)
    for r in records:
        mnc = r.metrics.get('mnc') or 'unknown'
        groups[str(mnc)].append(r)
    return dict(groups)

