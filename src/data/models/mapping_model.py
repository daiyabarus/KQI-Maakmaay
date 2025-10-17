from dataclasses import dataclass


@dataclass
class MappingModel:
    source_field: str
    target_field: str

