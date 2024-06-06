from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, NamedTuple, Optional, Tuple


class Mapper:
    def __init__(self) -> None:
        self._secondaries_by_primary: Dict[bytes, List[bytes]] = defaultdict(list)
        self._primary_by_secondary: Dict[bytes, bytes] = defaultdict(lambda: None)

    def map_secondary(self, secondary: bytes, primary: bytes) -> None:
        self._secondaries_by_primary[primary].append(secondary)
        self._primary_by_secondary[secondary] = primary

    def add_primary(self, primary: bytes) -> None:
        if primary not in self._secondaries_by_primary:
            self._secondaries_by_primary[primary] = []

    def get_primary(self, secondary: bytes) -> Optional[bytes]:
        if not self.is_secondary(secondary):
            return None
        return self._primary_by_secondary[secondary]
    
    def get_secondaries(self, primary: bytes) -> Optional[List[bytes]]:
        if not self.is_primary(primary):
            return None
        return self._secondaries_by_primary[primary]

    def is_primary(self, id: bytes) -> bool:
        return id in self._secondaries_by_primary

    def is_secondary(self, id: bytes) -> bool:
        return id in self._primary_by_secondary

    def is_known(self, id: bytes) -> bool:
        return self.is_primary(id) or self.is_secondary(id)