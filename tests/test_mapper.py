import time
import pytest

from geomerger.mapper import Mapper, MapperError,  ExpiringMapper


def test_map_secondary():
    testee = Mapper()
    
    testee.map_secondary(b'sec1', b'pri1')

    assert testee.is_primary(b'pri1') == True
    assert testee.is_secondary(b'pri1') == False
    assert testee.is_secondary(b'sec1') == True
    assert testee.is_primary(b'sec1') == False
    assert testee.is_secondary_for(b'sec1', b'pri1') == True

def test_map_secondary_idempotent():
    testee = Mapper()
    
    testee.map_secondary(b'sec1', b'pri1')
    testee.map_secondary(b'sec1', b'pri1')
    
    assert testee.is_primary(b'pri1') == True
    assert testee.is_secondary(b'sec1') == True
    assert testee.is_secondary_for(b'sec1', b'pri1') == True

def test_map_secondary_existing():
    testee = Mapper()
    
    testee.map_secondary(b'sec1', b'pri1')

    with pytest.raises(MapperError):
        testee.map_secondary(b'sec1', b'pri2')

    assert testee.is_primary(b'pri1') == True
    assert testee.is_secondary(b'sec1') == True
    assert testee.is_secondary_for(b'sec1', b'pri1') == True

def test_remap_secondary():
    testee = Mapper()
    
    testee.map_secondary(b'sec1', b'pri1')
    testee.remap_secondary(b'sec1', b'pri2')

    assert testee.is_primary(b'pri2') == True
    assert testee.is_secondary(b'sec1') == True
    assert testee.is_secondary_for(b'sec1', b'pri2') == True

def test_demote_primary():
    testee = Mapper()
    
    testee.map_secondary(b'sec1', b'pri1')
    testee.map_secondary(b'sec2', b'pri1')

    assert testee.get_secondaries(b'pri1') == [b'sec1', b'sec2']
    
    testee.demote_primary(b'pri1', new_primary=b'pri0', migrate_children=False)

    assert testee.is_secondary_for(b'pri1', b'pri0')
    assert testee.get_secondaries(b'pri0') == [b'pri1']

    assert testee.is_secondary(b'sec1') == False
    assert testee.is_secondary(b'sec2') == False

def test_demote_primary_migrate():
    testee = Mapper()
    
    testee.map_secondary(b'sec1', b'pri1')
    testee.map_secondary(b'sec2', b'pri1')

    assert testee.get_secondaries(b'pri1') == [b'sec1', b'sec2']
    
    testee.demote_primary(b'pri1', new_primary=b'pri0', migrate_children=True)

    assert testee.is_secondary_for(b'pri1', b'pri0')
    assert testee.get_secondaries(b'pri0') == [b'pri1', b'sec1', b'sec2']

def test_expiration():
    testee = ExpiringMapper(id_expiration_age_s=0.2)
    testee.map_secondary(b'sec1', b'pri1')
    testee.map_secondary(b'sec2', b'pri1')

    assert testee.is_known(b'pri1') == True

    time.sleep(0.5)
    
    testee.map_secondary(b'sec3', b'pri2')

    assert testee.is_known(b'pri1') == False
    assert testee.is_known(b'pri2') == True