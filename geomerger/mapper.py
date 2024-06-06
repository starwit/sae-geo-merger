from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, NamedTuple, Optional, Tuple
import uuid

def print_dict(d: Dict):
    for k, v in d.items():
        print(f'{k.hex()[:4]}:')
        if isinstance(v, list):
            for e in v:
                print(f'  {e.hex()[:4]}')
        else:
            print(f'  {v.hex()[:4]}')

class Mapper:
    '''All mutating operations are idempotent'''
    
    def __init__(self) -> None:
        self._secondaries_by_primary: Dict[bytes, List[bytes]] = defaultdict(list)
        self._primary_by_secondary: Dict[bytes, bytes] = defaultdict(lambda: None)

    def map_secondary(self, secondary: bytes, primary: bytes) -> None:
        '''Add a mapping from primary to secondary if it does not exist yet. 
        If secondary is already a primary, it is "demoted" first and its mappings are removed.'''

        if not self.is_primary(primary) or self.is_secondary_for(secondary, primary):
            return
        
        if self.is_primary(secondary):
            self.remove_primary(secondary)
        
        self._secondaries_by_primary[primary].append(secondary)
        self._primary_by_secondary[secondary] = primary

        print('sec_by_prim')
        print_dict(self._secondaries_by_primary)
        print('prim_by_sec')
        print_dict(self._primary_by_secondary)

    def add_primary(self, primary: bytes) -> None:
        if primary not in self._secondaries_by_primary:
            self._secondaries_by_primary[primary] = []

    def remove_primary(self, primary: bytes) -> None:
        if not self.is_primary(primary):
            return

        for sec in self._secondaries_by_primary[primary]:
            del self._primary_by_secondary[sec]
        
        del self._secondaries_by_primary[primary]

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
    
    def is_secondary_for(self, id: bytes, primary: bytes) -> bool:
        return self.is_primary(primary) and id in self._secondaries_by_primary[primary]

    def is_known(self, id: bytes) -> bool:
        return self.is_primary(id) or self.is_secondary(id)