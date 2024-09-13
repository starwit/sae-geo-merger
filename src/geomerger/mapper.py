import inspect
import logging
import time
from collections import defaultdict
from typing import Dict, List, NamedTuple

from ratelimit import limits

logger = logging.getLogger(__name__)


def dict_to_text(d: Dict):
    text = ''
    for k, v in d.items():
        text += f'\n{k}:'
        if isinstance(v, list):
            for e in v:
                text += f'\n  {e}'
        else:
            text += f'\n  {v}'
    return text

def id_to_str(id: bytes) -> str:
    return bytes.hex(id)[:4]


class MapperError(Exception):
    pass


class MapperEntry(NamedTuple):
    source_id: str
    object_id: bytes

    def __repr__(self):
        return f'[sid={self.source_id}, oid={id_to_str(self.object_id)}]'


class Mapper:
    '''
    This is essentially a collection of small trees with a deliberately limited set of operations available.
    All mutating operations are idempotent.
    '''
    
    def __init__(self) -> None:
        self._secondaries_by_primary: Dict[MapperEntry, List[MapperEntry]] = defaultdict(list)
        self._primary_by_secondary: Dict[MapperEntry, MapperEntry] = defaultdict(lambda: None)

    def map_secondary(self, secondary: MapperEntry, primary: MapperEntry) -> None:
        '''Add a mapping from primary to secondary if it does not exist yet.'''
        if primary.source_id == secondary.source_id:
            raise MapperError(f'Primary {primary} and secondary {secondary} have the same source_id')

        if self.is_secondary(secondary):
            if self.is_secondary_for(secondary, primary):
                # Is already correctly mapped.
                return
            else:
                other_primary = self.get_primary(secondary)
                raise MapperError(f'Secondary {secondary} already mapped to primary {other_primary}')
        
        self._add_mapping(primary, secondary)

        self._log_mappings()

    def _add_mapping(self, primary: MapperEntry, secondary: MapperEntry) -> None:
        self._secondaries_by_primary[primary].append(secondary)
        self._primary_by_secondary[secondary] = primary

    def _remove_primary(self, primary: MapperEntry, remove_secondaries = False) -> None:
        secondaries = self._secondaries_by_primary.pop(primary, None)
        if remove_secondaries and secondaries is not None:
            for sec in secondaries:
                self._primary_by_secondary.pop(sec, None)

    def _remove_secondary(self, secondary: MapperEntry, remove_primary = False) -> None:
        primary = self._primary_by_secondary.pop(secondary, None)
        if remove_primary and primary is not None:
            self._primary_by_secondary[secondary]

    def remap_secondary(self, secondary: MapperEntry, new_primary: MapperEntry) -> None:
        if secondary.source_id == new_primary.source_id:
            raise MapperError(f'Secondary {secondary} and new_primary {new_primary} have the same source_id')
        
        if not self.is_secondary(secondary):
            raise MapperError(f'{secondary} is not secondary.')

        if self.is_secondary_for(secondary, new_primary):
            # Correct, do nothing
            return
        
        old_primary = self.get_primary(secondary)
        self._secondaries_by_primary[old_primary].remove(secondary)
        self._secondaries_by_primary[new_primary].append(secondary)
        self._primary_by_secondary[secondary] = new_primary

        self._log_mappings()

    def demote_primary(self, primary: MapperEntry, new_primary: MapperEntry, migrate_children: bool = False) -> None:
        '''Demotes primary, by remapping it to new_primary as a secondary and migrates the children if needed.'''
        if primary.source_id == new_primary.source_id:
            raise MapperError(f'Primary {primary} and new_primary {new_primary} have the same source_id')
        
        if not self.is_primary(primary) or self.is_secondary(new_primary):
            raise MapperError(f'Primary {primary} or new primary {new_primary} is not primary.')

        children = []
        if migrate_children:
            children.extend(self._secondaries_by_primary [primary])

        self._remove_primary(primary, remove_secondaries=True)

        self._add_mapping(new_primary, primary)
        for child in children:
            self._add_mapping(new_primary, child)

        self._log_mappings()

    def get_primary(self, secondary: MapperEntry) -> MapperEntry:
        if not self.is_secondary(secondary):
            raise MapperError(f'Secondary {secondary} is not secondary.')
        return self._primary_by_secondary[secondary]
    
    def get_secondaries(self, primary: MapperEntry) -> List[MapperEntry]:
        if not self.is_primary(primary):
            raise MapperError(f'Primary {primary} is not primary.')
        return self._secondaries_by_primary[primary]

    def is_primary(self, entry: MapperEntry) -> bool:
        return entry in self._secondaries_by_primary

    def is_secondary(self, entry: MapperEntry) -> bool:
        return entry in self._primary_by_secondary
    
    def is_secondary_for(self, secondary: MapperEntry, primary: MapperEntry) -> bool:
        return self.is_primary(primary) and secondary in self._secondaries_by_primary[primary]

    def is_known(self, entry: MapperEntry) -> bool:
        return self.is_primary(entry) or self.is_secondary(entry)
    
    def _log_mappings(self) -> None:
        logger.warning(dict_to_text(self._secondaries_by_primary))
    

class ExpiringMapper(Mapper):
    def __init__(self, entry_expiration_age_s: int = 30) -> None:
        super().__init__()

        self._entry_expiration_age_s = entry_expiration_age_s
        self._entries_last_seen: Dict[MapperEntry, float] = {}
        self._expire_entries_limited = limits(calls=10, period=self._entry_expiration_age_s, raise_on_limit=False)(self._expire_entries)
        
        # Wrap all methods part of the public API to transparently track and expire entries
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if not name.startswith('_'):
                setattr(self, name, self._wrap_method(method))
 
    def _wrap_method(self, method):
        def wrapper(*args, **kwargs):
            for arg in [*args, *kwargs.values()]:
                if isinstance(arg, MapperEntry):
                    self._entries_last_seen[arg] = time.time()
            self._expire_entries_limited()
            return method(*args, **kwargs)
        return wrapper
    
    def _expire_entries(self):
        expired_entries = [entry for entry, last_seen in self._entries_last_seen.items() if time.time() - last_seen > self._entry_expiration_age_s]
        for entry in expired_entries:
            # TODO The calls to self.x recurse, because the methods are wrapped with this method. We should automatically add _versions of these wrapped methods without the wrapper
            # if self.is_primary(entry):
            #     for sec in self.get_secondaries(entry):
            #         if sec in expired_entries:
            #             self._remove_secondary(sec)
            #     if len(self.get_secondaries(entry)) == 0:
            #         self._remove_primary(entry)
            # if self.is_secondary(entry):
            #     self._remove_secondary(entry)
            self._entries_last_seen.pop(entry, None)
