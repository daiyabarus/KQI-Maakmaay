from ...domain.use_cases.group_by_mnc_usecase import group_by_mnc
from ...domain.use_cases.calculate_metrics_usecase import calculate_metrics


def aggregate_by_mnc(records):
    groups = group_by_mnc(records)
    results = {}
    for mnc, recs in groups.items():
        results[mnc] = calculate_metrics(recs)
    return results

