from typing import Iterable
from ...domain.repositories.mapping_repository import MappingRepository
from ...domain.entities.source_mapping import SourceMapping


class MappingRepositoryImpl(MappingRepository):
    def __init__(self, rows_iterable: Iterable[dict]):
        self._rows = rows_iterable

    def load_mappings(self) -> Iterable[SourceMapping]:
        for r in self._rows:
            src = r.get('source_field') or r.get('source')
            tgt = r.get('target_field') or r.get('target')
            if src and tgt:
                yield SourceMapping(source_field=src, target_field=tgt)

