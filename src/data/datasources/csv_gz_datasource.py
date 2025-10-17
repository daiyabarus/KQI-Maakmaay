import gzip
import csv
from typing import Iterable, Mapping


def iter_csv_gz_rows(path: str) -> Iterable[Mapping[str, str]]:
    """Yield rows from a .csv.gz file as dicts."""
    with gzip.open(path, mode='rt', encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            yield row

