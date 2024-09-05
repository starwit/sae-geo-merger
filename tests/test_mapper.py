import time

import pytest

from geomerger.mapper import ExpiringMapper, Mapper
from geomerger.mapper import MapperEntry as ME
from geomerger.mapper import MapperError


def test_map_secondary():
    testee = Mapper()

    prim = ME('s1', b'pri1')
    sec = ME('s2', b'sec1')
    
    testee.map_secondary(sec, prim)

    assert testee.is_primary(prim) == True
    assert testee.is_secondary(prim) == False
    assert testee.is_secondary(sec) == True
    assert testee.is_primary(sec) == False
    assert testee.is_secondary_for(sec, prim) == True

def test_map_secondary_idempotent():
    testee = Mapper()
    
    prim = ME('s1', b'pri1')
    sec = ME('s2', b'sec1')
    
    testee.map_secondary(sec, prim)
    testee.map_secondary(sec, prim)
    
    assert testee.is_primary(prim) == True
    assert testee.is_secondary(sec) == True
    assert testee.is_secondary_for(sec, prim) == True

def test_map_secondary_existing():
    testee = Mapper()
    
    prim = ME('s1', b'pri1')
    prim2 = ME('s1', b'pri2')
    sec = ME('s2', b'sec1')
    
    testee.map_secondary(sec, prim)

    with pytest.raises(MapperError):
        testee.map_secondary(sec, prim2)

    assert testee.is_primary(prim) == True
    assert testee.is_secondary(sec) == True
    assert testee.is_secondary_for(sec, prim) == True

def test_remap_secondary():
    testee = Mapper()
    
    prim = ME('s1', b'pri1')
    prim2 = ME('s1', b'pri2')
    sec = ME('s2', b'sec1')
    
    testee.map_secondary(sec, prim)
    testee.remap_secondary(sec, prim2)

    assert testee.is_primary(prim2) == True
    assert testee.is_secondary(sec) == True
    assert testee.is_secondary_for(sec, prim2) == True

def test_demote_primary():
    testee = Mapper()
    
    prim = ME('s1', b'pri1')
    prim2 = ME('s3', b'pri2')
    sec = ME('s2', b'sec1')
    sec2 = ME('s2', b'sec2')
    
    testee.map_secondary(sec, prim)
    testee.map_secondary(sec2, prim)

    assert testee.get_secondaries(prim) == [sec, sec2]
    
    testee.demote_primary(prim, new_primary=prim2, migrate_children=False)

    assert testee.is_secondary_for(prim, prim2)
    assert testee.get_secondaries(prim2) == [prim]

    assert testee.is_secondary(sec) == False
    assert testee.is_secondary(sec2) == False

def test_demote_primary_migrate():
    testee = Mapper()

    prim = ME('s1', b'pri1')
    prim2 = ME('s3', b'pri2')
    sec = ME('s2', b'sec1')
    sec2 = ME('s2', b'sec2')
    
    testee.map_secondary(sec, prim)
    testee.map_secondary(sec2, prim)

    assert testee.get_secondaries(prim) == [sec, sec2]
    
    testee.demote_primary(prim, new_primary=prim2, migrate_children=True)

    assert testee.is_secondary_for(prim, prim2)
    assert testee.get_secondaries(prim2) == [prim, sec, sec2]

def test_source_constraint():
    testee = Mapper()

    prim = ME('s1', b'pri1')
    sec = ME('s1', b'sec1')

    with pytest.raises(MapperError):
        testee.map_secondary(sec, prim)

    assert testee.is_known(sec) == False
    assert testee.is_known(prim) == False

def test_source_constraint_remap():
    testee = Mapper()

    prim = ME('s1', b'pri1')
    prim2 = ME('s2', b'pri2')
    sec = ME('s2', b'sec1')

    testee.map_secondary(sec, prim)

    with pytest.raises(MapperError):
        testee.remap_secondary(sec, prim2)

    assert testee.is_secondary_for(sec, prim)
    assert testee.is_known(prim2) == False

def test_expiration():
    testee = ExpiringMapper(entry_expiration_age_s=0.2)

    prim = ME('s1', b'pri1')
    prim2 = ME('s1', b'pri2')
    sec = ME('s2', b'sec1')
    sec2 = ME('s2', b'sec2')
    sec3 = ME('s2', b'sec3')
    
    testee.map_secondary(sec, prim)
    testee.map_secondary(sec2, prim)

    assert testee.is_known(prim) == True

    time.sleep(0.5)
    
    testee.map_secondary(sec3, prim2)

    assert testee.is_known(prim) == False
    assert testee.is_known(prim2) == True
