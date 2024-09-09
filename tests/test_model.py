from datetime import datetime

from geomerger.geo import Coord
from geomerger.model import ObjectPositionModel


def test_object_position_model_extrapolation():
    testee = ObjectPositionModel(b'obj1')

    testee.observe(Coord(10,20), datetime.fromtimestamp(0))
    testee.observe(Coord(11,22), datetime.fromtimestamp(10))

    assert testee.get_position(datetime.fromtimestamp(-5)) == Coord(9.5, 19)
    assert testee.get_position(datetime.fromtimestamp(0)) == Coord(10, 20)
    assert testee.get_position(datetime.fromtimestamp(5)) == Coord(10.5, 21)
    assert testee.get_position(datetime.fromtimestamp(10)) == Coord(11, 22)
    assert testee.get_position(datetime.fromtimestamp(15)) == Coord(11.5, 23)