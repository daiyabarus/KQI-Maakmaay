from typing import Iterable
from ...data.datasources.kqi_datasource import load_kqi_from_rows


def parse_rows(rows: Iterable[dict]):
    return load_kqi_from_rows(rows)

