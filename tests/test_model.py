from datetime import datetime

from geomerger.geo import Coord
from geomerger.model import CameraAreaModel, ObjectPositionModel


def test_object_position_model_extrapolation():
    testee = ObjectPositionModel(b'obj1')

    testee.observe(Coord(10,20), datetime.fromtimestamp(0))
    testee.observe(Coord(11,22), datetime.fromtimestamp(10))

    assert testee.get_position(datetime.fromtimestamp(-5)) == Coord(9.5, 19)
    assert testee.get_position(datetime.fromtimestamp(0)) == Coord(10, 20)
    assert testee.get_position(datetime.fromtimestamp(5)) == Coord(10.5, 21)
    assert testee.get_position(datetime.fromtimestamp(10)) == Coord(11, 22)
    assert testee.get_position(datetime.fromtimestamp(15)) == Coord(11.5, 23)

def test_camera_area_model():
    testee = CameraAreaModel('cam1')
    
    testee.observe_object(b'obj1', Coord(0,0), datetime.fromtimestamp(0))
    testee.observe_object(b'obj1', Coord(1,0), datetime.fromtimestamp(10))
    testee.observe_object(b'obj2', Coord(20,0), datetime.fromtimestamp(0))
    testee.observe_object(b'obj2', Coord(21,0), datetime.fromtimestamp(10))

    assert testee.find_closest_object(Coord(2,0), datetime.fromtimestamp(0)) == (b'obj1', 2.0)
    assert testee.find_closest_object(Coord(2,0), datetime.fromtimestamp(10)) == (b'obj1', 1.0)
    assert testee.find_closest_object(Coord(11,0), datetime.fromtimestamp(0)) == (b'obj2', 9.0)


