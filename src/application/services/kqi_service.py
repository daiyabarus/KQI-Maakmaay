from ...domain.use_cases.process_kqi_usecase import process_kqi
from ...domain.use_cases.calculate_metrics_usecase import calculate_metrics
from ...domain.repositories.kqi_repository import KQIRepository


def run_kqi_pipeline(kqi_repo: KQIRepository):
    records = kqi_repo.stream_kqi()
    processed = list(process_kqi(records))
    metrics = calculate_metrics(processed)
    return processed, metrics

