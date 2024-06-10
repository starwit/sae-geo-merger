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

def id_to_str(id: bytes) -> str:
    return bytes.hex(id)[:4]


class MapperError(Exception):
    pass


class Mapper:
    '''All mutating operations are idempotent'''
    
    def __init__(self) -> None:
        self._secondaries_by_primary: Dict[bytes, List[bytes]] = defaultdict(list)
        self._primary_by_secondary: Dict[bytes, bytes] = defaultdict(lambda: None)

    def map_secondary(self, secondary: bytes, primary: bytes) -> None:
        '''Add a mapping from primary to secondary if it does not exist yet.'''

        if self.is_secondary(secondary):
            if self.is_secondary_for(secondary, primary):
                # Is already correctly mapped.
                return
            else:
                other_primary = self.get_primary(secondary)
                raise MapperError(f'Secondary {id_to_str(secondary)} already mapped to primary {id_to_str(other_primary)}')
        
        self._add_mapping(primary, secondary)

        print('sec_by_prim')
        print_dict(self._secondaries_by_primary)
        print('prim_by_sec')
        print_dict(self._primary_by_secondary)

    def _add_mapping(self, primary: bytes, secondary: bytes) -> None:
        self._secondaries_by_primary[primary].append(secondary)
        self._primary_by_secondary[secondary] = primary

    def _remove_primary(self, primary: bytes) -> None:
        secondaries = self._secondaries_by_primary.pop(primary, None)
        if secondaries is not None:
            for sec in secondaries:
                self._primary_by_secondary.pop(sec, None)

    def _remove_secondary(self, secondary: bytes) -> None:
        primary = self._primary_by_secondary.pop(secondary, None)
        if primary is not None:
            self._primary_by_secondary[secondary]

    def remap_secondary(self, secondary: bytes, new_primary: bytes) -> None:
        if not self.is_secondary(secondary):
            raise MapperError(f'{id_to_str(secondary)} is not secondary.')

        if self.is_secondary_for(secondary, new_primary):
            # Correct, do nothing
            return
        
        old_primary = self.get_primary(secondary)
        self._secondaries_by_primary[old_primary].remove(secondary)
        self._secondaries_by_primary[new_primary].append(secondary)
        self._primary_by_secondary[secondary] = new_primary

    def demote_primary(self, primary: bytes, new_primary: bytes, migrate_children: bool = False) -> None:
        '''Demotes primary, by remapping it to new_primary as a secondary and migrates the children if needed.'''
        if not self.is_primary(primary) or not self.is_primary(new_primary):
            raise MapperError(f'Primary {id_to_str(primary)} or new primary {id_to_str(new_primary)} is not primary.')

        children = []
        if migrate_children:
            children.extend(self._secondaries_by_primary(primary))

        self._remove_primary(primary)

        self._add_mapping(new_primary, primary)
        for child in children:
            self._add_mapping(new_primary, child)

    def get_primary(self, secondary: bytes) -> bytes:
        if not self.is_secondary(secondary):
            raise MapperError(f'Secondary {id_to_str(secondary)} is not secondary.')
        return self._primary_by_secondary[secondary]
    
    def get_secondaries(self, primary: bytes) -> List[bytes]:
        if not self.is_primary(primary):
            raise MapperError(f'Primary {id_to_str(primary)} is not primary.')
        return self._secondaries_by_primary[primary]

    def is_primary(self, id: bytes) -> bool:
        return id in self._secondaries_by_primary

    def is_secondary(self, id: bytes) -> bool:
        return id in self._primary_by_secondary
    
    def is_secondary_for(self, id: bytes, primary: bytes) -> bool:
        return self.is_primary(primary) and id in self._secondaries_by_primary[primary]

    def is_known(self, id: bytes) -> bool:
        return self.is_primary(id) or self.is_secondary(id)