import time
from collections import deque
from datetime import datetime
from typing import Deque, Dict, NamedTuple, Optional, Tuple

from .geo import Coord


class Position(NamedTuple):
    time: datetime
    coord: Coord


# TODO find a good way to expire objects
class CameraAreaModel:
    '''
        This class represents the area (in geo-coordinate space) covered by a single camera. It must be fed object observations and is NOT class aware.
        It's purpose is to keep track of these objects and provide efficient location queries (i.e. "which is the closest object to loc x?")
    '''
    def __init__(self, id: str) -> None:
        self._id = id
        self._objects: Dict[bytes, ObjectPositionModel] = dict()

    def observe_object(self, object_id: bytes, coord: Coord, time: datetime) -> None:
        if object_id not in self._objects:
            self._objects[object_id] = ObjectPositionModel(object_id)
        self._objects[object_id].observe(coord, time)

    def find_closest_object(self, ref_coord: Coord, at_time: datetime) -> Optional[Tuple[bytes, float]]:
        '''Finds closest object to ref_coord by euclidean distance in coord space.'''
        distance = float('inf')
        id = None
        for obj in self._objects.values():
            cur_distance = self._get_distance(ref_coord, obj.get_position(at_time))
            if cur_distance < distance:
                distance = cur_distance
                id = obj.id
        
        return id, distance
    
    def _get_distance(self, c1: Coord, c2: Coord) -> float:
        return ((c1.lat - c2.lat) ** 2 + (c1.lon - c2.lon) ** 2) ** 0.5


class ObjectPositionModel:
    '''
        This class represents a simple object position. It must be fed with object observations (from the same camera!).
        It also provides methods to query the position for a given time, i.e. inter/extrapolate positions.
    '''
    def __init__(self, id: bytes) -> None:
        self._id = id
        self._positions: Deque[Position] = deque(maxlen=2)
        self._last_update_ts: float = time.time()

    @property
    def id(self) -> bytes:
        return self._id
    
    def observe(self, coord: Coord, at_time: datetime) -> None:
        self._positions.append(Position(at_time, coord))
        self._last_update_ts = time.time()

    def get_position(self, at_time: datetime) -> Optional[Coord]:
        if len(self._positions) == 0:
            return None
        if len(self._positions) == 1:
            return self._positions[-1].coord
        
        ref = self._positions[-1]
        prev = self._positions[-2]

        speed_lat, speed_lon = self._get_speed(prev, ref)

        delta_t = (at_time - ref.time).total_seconds()

        calc_lat = ref.coord.lat + (speed_lat * delta_t)
        calc_lon = ref.coord.lon + (speed_lon * delta_t)

        return Coord(calc_lat, calc_lon)

    def _get_speed(self, pt1: Position, pt2: Position) -> Tuple[float, float]:
        delta_lat = pt2.coord.lat - pt1.coord.lat
        delta_lon = pt2.coord.lon - pt1.coord.lon

        delta_t = pt2.time.timestamp() - pt1.time.timestamp()

        speed_lat = delta_lat / delta_t
        speed_lon = delta_lon / delta_t

        return speed_lat, speed_lon

    def last_updated(self) -> float:
        return self._last_update_ts