import os
from typing import Iterable


def list_data_files(directory: str, extensions=('.csv.gz', '.csv')) -> Iterable[str]:
    for root, _, files in os.walk(directory):
        for f in files:
            if f.endswith(extensions):
                yield os.path.join(root, f)

