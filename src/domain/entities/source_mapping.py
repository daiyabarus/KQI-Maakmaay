from dataclasses import dataclass


@dataclass
class SourceMapping:
    """Mapping from source fields to canonical field names.

    Example:
        SourceMapping(source_field='CellID', target_field='cell')
    """

    source_field: str
    target_field: str

