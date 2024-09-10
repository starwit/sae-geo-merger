import time
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List, NamedTuple, Optional, Tuple

from .geo import Coord


class Observation(NamedTuple):
    time: datetime
    coord: Coord


class ObservedObject(NamedTuple):
    id: bytes
    obs: Observation


class CameraAreaObservation(NamedTuple):
    id: str
    objects: List[ObservedObject]


class SimilarityStatement(NamedTuple):
    candidates: List[bytes]
    time: datetime


class SimilarityTracker:
    def __init__(self) -> None:
        self._similarity_statements = List[SimilarityStatement]

    def add_candidates(self, candidate_ids: List[bytes], at_time: datetime):
        self._similarity_statements.append(SimilarityStatement(at_time, candidate_ids))
        # TODO check up to a certain time delta if some candidate combinations appear, if yes, merge them (where do we do that? here?)


class AreaModel:
    def __init__(self, merging_threshold_m: float) -> None:
        self._merging_threshold_m = merging_threshold_m
        self._similarity_tracker = SimilarityTracker()
        self._cam_models: Dict[str, CameraAreaModel] = dict()
        self._last_update_ts = time.time()
        self._most_recent_obs_dt = datetime.fromtimestamp(0)

    # IDEA implement similarity search on every observe, that way we know that we process every observation only once
    def observe(self, msg: CameraAreaObservation):
        if msg.id not in self._cam_models:
            self._cam_models[msg.id] = CameraAreaModel(msg.id)
        for obs in msg.objects:
            self._cam_models[msg.id].observe_object(obs)
            if obs.obs.time > self._most_recent_obs_dt:
                self._most_recent_obs_dt = obs.obs.time
        self._last_update_ts = time.time()

    def current_time(self) -> datetime:
        '''Calculates the current model time (based on most recent update and elapsed time since then)'''
        elapsed_time = time.time() - self._last_update_ts
        return self._most_recent_obs_dt + timedelta(seconds=elapsed_time)
    
    def _find_closest_objects(self, ref_coord: Coord, ref_camera_id: str, at_time: datetime) -> List[Tuple[bytes, float]]:
        candidates: List[Tuple[bytes, float]] = []
        for cam in [c for c in self._cam_models.values() if c.id != ref_camera_id]:
            id, distance = cam.find_closest_object(ref_coord, at_time)
            if id is not None:
                candidates.append((id, distance))
        return candidates


# TODO find a good way to expire objects. That we definitely need!
class CameraAreaModel:
    '''
        This class represents the area (in geo-coordinate space) covered by a single camera. It must be fed object observations and is NOT class aware.
        It's purpose is to keep track of these objects and provide efficient location queries (i.e. "which is the closest object to loc x?")
    '''
    def __init__(self, id: str) -> None:
        self._id = id
        self._objects: Dict[bytes, ObjectPositionModel] = dict()

    def observe_object(self, obs: ObservedObject) -> None:
        if obs.id not in self._objects:
            self._objects[obs.id] = ObjectPositionModel(obs.id)
        self._objects[obs.id].observe(obs.obs.coord, time)

    def find_closest_object(self, ref_coord: Coord, at_time: datetime) -> Optional[Tuple[bytes, float]]:
        '''Finds closest object to ref_coord by euclidean distance in coord space.'''
        distance = float('inf')
        id = None
        for obj in self._objects.values():
            # TODO Check if last object observation is higher than the expiring threshold and then delete it
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
        self._positions: Deque[Observation] = deque(maxlen=2)
        self._last_update_ts: float = time.time()

    @property
    def id(self) -> bytes:
        return self._id
    
    def observe(self, coord: Coord, at_time: datetime) -> None:
        self._positions.append(Observation(at_time, coord))
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

    def _get_speed(self, pt1: Observation, pt2: Observation) -> Tuple[float, float]:
        delta_lat = pt2.coord.lat - pt1.coord.lat
        delta_lon = pt2.coord.lon - pt1.coord.lon

        delta_t = pt2.time.timestamp() - pt1.time.timestamp()

        speed_lat = delta_lat / delta_t
        speed_lon = delta_lon / delta_t

        return speed_lat, speed_lon

    def last_updated(self) -> float:
        return self._last_update_ts