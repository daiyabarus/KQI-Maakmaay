from typing import Iterable
from ...domain.entities.source_mapping import SourceMapping


def load_mappings_from_rows(rows: Iterable[dict]) -> Iterable[SourceMapping]:
    for r in rows:
        src = r.get('source_field') or r.get('source')
        tgt = r.get('target_field') or r.get('target')
        if src and tgt:
            yield SourceMapping(source_field=src, target_field=tgt)

