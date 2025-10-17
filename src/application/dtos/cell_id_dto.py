from dataclasses import dataclass


@dataclass
class CellIDDTO:
    mcc: int
    mnc: int
    enodeb: int
    cell_id: int

