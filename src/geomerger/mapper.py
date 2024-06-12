import time
from collections import defaultdict
from typing import Any, Dict, List
from ratelimit import limits
import inspect


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
    '''
    This is essentially a collection of small trees with a deliberately limited set of operations available.
    All mutating operations are idempotent.
    '''
    
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
        if not self.is_primary(primary) or self.is_secondary(new_primary):
            raise MapperError(f'Primary {id_to_str(primary)} or new primary {id_to_str(new_primary)} is not primary.')

        children = []
        if migrate_children:
            children.extend(self._secondaries_by_primary [primary])

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
    

class ExpiringMapper(Mapper):
    def __init__(self, id_expiration_age_s: int = 120) -> None:
        super().__init__()

        self._id_expiration_age_s = id_expiration_age_s
        self._ids_last_seen: Dict[bytes, float] = {}
        self._expire_ids_limited = limits(calls=10, period=self._id_expiration_age_s, raise_on_limit=False)(self._expire_ids)
        
        # Wrap all methods part of the public API to transparently track and expire ids
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if not name.startswith('_'):
                setattr(self, name, self._wrap_method(method))
 
    def _wrap_method(self, method):
        def wrapper(*args, **kwargs):
            for arg in [*args, *kwargs.values()]:
                if isinstance(arg, bytes):
                    self._ids_last_seen[arg] = time.time()
            self._expire_ids_limited()
            return method(*args, **kwargs)
        return wrapper
    
    def _expire_ids(self):
        expired_ids = [id for id, last_seen in self._ids_last_seen.items() if time.time() - last_seen > self._id_expiration_age_s]
        for id in expired_ids:
            self._remove_primary(id)
            self._remove_secondary(id)
            self._ids_last_seen.pop(id, None)
