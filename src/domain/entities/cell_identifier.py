from dataclasses import dataclass


@dataclass(frozen=True)
class CellIdentifier:
    """Represents a cell identifier composed of MCC, MNC, eNodeB and Cell ID.

    All fields are stored as integers where applicable.
    """

    mcc: int
    mnc: int
    enodeb: int
    cell_id: int

    def full_id(self) -> str:
        return f"{self.mcc}-{self.mnc}-{self.enodeb}-{self.cell_id}"

