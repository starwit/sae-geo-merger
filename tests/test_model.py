from datetime import datetime
import time

from geomerger.geo import Coord
from geomerger.model import (AreaModel, CameraAreaModel, CameraAreaObservation,
                             ObjectPositionModel, Observation, ObservedObject)


def test_object_position_model_extrapolation():
    testee = ObjectPositionModel(b'obj1')

    testee.observe(Coord(10,20), datetime.fromtimestamp(0))
    testee.observe(Coord(11,22), datetime.fromtimestamp(10))

    assert testee.get_position(datetime.fromtimestamp(-5)) == Coord(9.5, 19)
    assert testee.get_position(datetime.fromtimestamp(0)) == Coord(10, 20)
    assert testee.get_position(datetime.fromtimestamp(5)) == Coord(10.5, 21)
    assert testee.get_position(datetime.fromtimestamp(10)) == Coord(11, 22)
    assert testee.get_position(datetime.fromtimestamp(15)) == Coord(11.5, 23)
    
def test_object_position_model_special_cases():
    testee = ObjectPositionModel(b'obj1')

    assert testee.get_position(datetime.fromtimestamp(0)) == None

    testee.observe(Coord(10,20), datetime.fromtimestamp(0))

    assert testee.get_position(datetime.fromtimestamp(0)) == Coord(10, 20)

def test_object_position_model_identical_obs():
    testee = ObjectPositionModel(b'obj1')

    testee.observe(Coord(10,20), datetime.fromtimestamp(0))
    testee.observe(Coord(10,20), datetime.fromtimestamp(0))

    assert testee.get_position(datetime.fromtimestamp(-5)) == Coord(10, 20)
    assert testee.get_position(datetime.fromtimestamp(0)) == Coord(10, 20)
    assert testee.get_position(datetime.fromtimestamp(5)) == Coord(10, 20)
    assert testee.get_position(datetime.fromtimestamp(10)) == Coord(10, 20)
    assert testee.get_position(datetime.fromtimestamp(15)) == Coord(10, 20)

def test_camera_area_model():
    testee = CameraAreaModel('cam1')

    obs11 = ObservedObject(b'obj1', Observation(datetime.fromtimestamp(0), Coord(0,0)))
    obs12 = ObservedObject(b'obj1', Observation(datetime.fromtimestamp(10), Coord(1,0)))
    obs21 = ObservedObject(b'obj2', Observation(datetime.fromtimestamp(0), Coord(20,0)))
    obs22 = ObservedObject(b'obj2', Observation(datetime.fromtimestamp(10), Coord(21,0)))

    testee.observe_object(obs11)
    testee.observe_object(obs12)
    testee.observe_object(obs21)
    testee.observe_object(obs22)
    
    testee.find_closest_object(Coord(2,0), datetime.fromtimestamp(0)) == b'obj1'
    testee.find_closest_object(Coord(2,0), datetime.fromtimestamp(10)) == b'obj1'
    testee.find_closest_object(Coord(11,0), datetime.fromtimestamp(0)) == b'obj2'

def test_camera_area_model_expiration():
    testee = CameraAreaModel('cam1')

    obs11 = ObservedObject(b'obj1', Observation(datetime.fromtimestamp(0), Coord(0,0)))
    obs12 = ObservedObject(b'obj1', Observation(datetime.fromtimestamp(10), Coord(1,0)))
    obs21 = ObservedObject(b'obj2', Observation(datetime.fromtimestamp(0), Coord(20,0)))

    testee.observe_object(obs11)
    testee.observe_object(obs21)

    assert len(testee.get_all_objects(at_time=datetime.fromtimestamp(10))) == 2

    time.sleep(0.10)

    testee.observe_object(obs12)

    testee.expire_objects(expiration_age_s=0.05)

    assert len(testee.get_all_objects(at_time=datetime.fromtimestamp(10))) == 1

def test_area_model():
    testee = AreaModel(merging_threshold_m=1)

    obs = Observation(datetime.fromtimestamp(0), Coord(0,0))

    cao1 = CameraAreaObservation('cam1', [
        ObservedObject(b'obj11', obs),
        ObservedObject(b'obj12', obs),
    ])

    cao11 = CameraAreaObservation('cam1', [
        ObservedObject(b'obj11', obs),
        ObservedObject(b'obj13', obs),
    ])

    cao2 = CameraAreaObservation('cam2', [
        ObservedObject(b'obj21', obs),
        ObservedObject(b'obj22', obs),
    ])

    testee.observe(cao1)
    testee.observe(cao11)
    testee.observe(cao2)

    assert len(testee.get_all_observed_objects()) == 5
