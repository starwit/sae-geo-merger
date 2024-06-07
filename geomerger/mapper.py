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

def print_id(id: bytes) -> str:
    bytes.hex()[:4]


class MapperError(Exception):
    pass


class Mapper:
    '''All mutating operations are idempotent'''
    
    def __init__(self) -> None:
        self._secondaries_by_primary: Dict[bytes, List[bytes]] = defaultdict(list)
        self._primary_by_secondary: Dict[bytes, bytes] = defaultdict(lambda: None)

    def map_secondary(self, secondary: bytes, primary: bytes) -> None:
        '''Add a mapping from primary to secondary if it does not exist yet.'''

        if not self.is_primary(primary):
            raise MapperError(f'Primary {print_id(primary)} is not primary.')
        
        if self.is_secondary(secondary):
            if self.is_secondary_for(secondary, primary):
                # Is already correctly mapped.
                return
            else:
                other_primary = self.get_primary(secondary)
                raise MapperError(f'Secondary {print_id(secondary)} already mapped to primary {print_id(other_primary)}')
        
        self._secondaries_by_primary[primary].append(secondary)
        self._primary_by_secondary[secondary] = primary

        print('sec_by_prim')
        print_dict(self._secondaries_by_primary)
        print('prim_by_sec')
        print_dict(self._primary_by_secondary)

    def remap_secondary(self, secondary: bytes, new_primary: bytes) -> None:
        if not self.is_secondary(secondary):
            raise MapperError(f'{print_id(secondary)} is not secondary.')

        if self.is_secondary_for(secondary, new_primary):
            # Correct, do nothing
            return
        
        old_primary = self.get_primary(secondary)
        self._secondaries_by_primary[old_primary].remove(secondary)
        self._secondaries_by_primary[new_primary].append(secondary)
        self._primary_by_secondary[secondary] = new_primary

    def add_primary(self, primary: bytes) -> None:
        if primary not in self._secondaries_by_primary:
            self._secondaries_by_primary[primary] = []

    def demote_primary(self, primary: bytes, new_primary: bytes) -> None:
        if not self.is_primary(primary) or not self.is_primary(new_primary):
            raise MapperError(f'Primary {print_id(primary)} or new primary {print_id(new_primary)} is not primary.')

        for sec in self._secondaries_by_primary[primary]:
            del self._primary_by_secondary[sec]
        
        del self._secondaries_by_primary[primary]

    def get_primary(self, secondary: bytes) -> bytes:
        if not self.is_secondary(secondary):
            raise MapperError(f'Secondary {print_id(secondary)} is not secondary.')
        return self._primary_by_secondary[secondary]
    
    def get_secondaries(self, primary: bytes) -> List[bytes]:
        if not self.is_primary(primary):
            raise MapperError(f'Primary {print_id(primary)} is not primary.')
        return self._secondaries_by_primary[primary]

    def is_primary(self, id: bytes) -> bool:
        return id in self._secondaries_by_primary

    def is_secondary(self, id: bytes) -> bool:
        return id in self._primary_by_secondary
    
    def is_secondary_for(self, id: bytes, primary: bytes) -> bool:
        return self.is_primary(primary) and id in self._secondaries_by_primary[primary]

    def is_known(self, id: bytes) -> bool:
        return self.is_primary(id) or self.is_secondary(id)