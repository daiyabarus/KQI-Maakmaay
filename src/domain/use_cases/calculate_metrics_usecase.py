from typing import Iterable, Dict
from ..entities.kqi_raw_data import KQIRawData


def calculate_metrics(records: Iterable[KQIRawData]) -> Dict[str, float]:
    """Compute simple aggregate metrics from given records.

    Returns a dict with counts and placeholder averages.
    """
    count = 0
    sum_val = 0.0
    for r in records:
        count += 1
        val = r.metrics.get('value') or 0.0
        try:
            sum_val += float(val)
        except Exception:
            pass
    avg = (sum_val / count) if count else 0.0
    return {"count": count, "average_value": avg}

