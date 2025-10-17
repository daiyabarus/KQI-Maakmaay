from .services.kqi_service import run_kqi_pipeline


def run_pipeline(kqi_repo):
    processed, metrics = run_kqi_pipeline(kqi_repo)
    # In a real app we'd persist or further process the metrics
    print('Processed', len(processed), 'records')
    print('Metrics:', metrics)


if __name__ == '__main__':
    print('main_pipeline invoked directly; integrate with main.py')

