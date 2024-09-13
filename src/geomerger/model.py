import time
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Dict, List, NamedTuple, Optional, Tuple

from .geo import Coord, distance_m


class Observation(NamedTuple):
    time: datetime
    coord: Coord


class ObservedObject(NamedTuple):
    id: bytes
    obs: Observation

    def __repr__(self) -> str:
        return f'{self.id.hex()[:4]}({round(self.obs.coord.lat, 6)}, {round(self.obs.coord.lon, 6)})'


class CameraAreaObservation(NamedTuple):
    id: str
    objects: List[ObservedObject]

    
class ClusterEntry(NamedTuple):
    camera_id: str
    obj: ObservedObject
    distance: float


class AreaModel:
    def __init__(self, merging_threshold_m: float) -> None:
        self._merging_threshold_m = merging_threshold_m
        self._cam_models: Dict[str, CameraAreaModel] = dict()
        self._last_update_ts = time.time()
        self._most_recent_obs_dt = datetime.fromtimestamp(0)

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
    
    def get_all_observed_objects(self) -> List[Tuple[str, ObservedObject]]:
        '''Retrieves all observed objects at current model with corresponding camera id'''
        current_time = self.current_time()
        objects = []
        for cam in self._cam_models.values():
            objects += [(cam.id, obj) for obj in cam.get_all_objects(current_time)]
        return objects
    
    def find_object_clusters(self, at_time: datetime) -> List[List[ClusterEntry]]:
        clusters: List[List[ClusterEntry]] = []
        seen_clusters = set()
        objects_by_cam = {c: m.get_all_objects(at_time) for c, m in self._cam_models.items()}
        for cam, objects in objects_by_cam.items():
            for obj in objects:
                matches = self._find_matching_objects(obj.obs.coord, cam, at_time)
                if len(matches) > 0:
                    cluster_candidate = [ClusterEntry(cam, obj, 0)] + matches
                    if (h := self._cluster_hash(cluster_candidate)) not in seen_clusters:
                        clusters.append(cluster_candidate)
                        seen_clusters.add(h)
        return clusters
    
    def _cluster_hash(self, cluster: List[ClusterEntry]) -> int:
        hash_acc = 0
        for entry in cluster:
            hash_acc += hash(entry.obj.id)
        return hash_acc
    
    def _find_matching_objects(self, ref_coord: Coord, ref_camera_id: str, at_time: datetime) -> List[ClusterEntry]:
        candidates: List[ClusterEntry] = []
        for cam in [c for c in self._cam_models.values() if c.id != ref_camera_id]:
            obj, distance = cam.find_closest_object(ref_coord, at_time)
            if obj is not None and distance < self._merging_threshold_m:
                candidates.append(ClusterEntry(cam.id, obj, distance))
        return candidates
    
    def expire_objects(self, expiration_age_s: float) -> None:
        for cam in self._cam_models.values():
            cam.expire_objects(expiration_age_s)


class CameraAreaModel:
    '''
        This class represents the area (in geo-coordinate space) covered by a single camera. It must be fed object observations and is NOT class aware.
        It's purpose is to keep track of these objects and provide efficient location queries (i.e. "which is the closest object to loc x?")
    '''
    def __init__(self, id: str) -> None:
        self._id = id
        self._objects: Dict[bytes, ObjectPositionModel] = dict()

    @property
    def id(self) -> str:
        return self._id

    def get_all_objects(self, at_time: datetime) -> List[ObservedObject]:
        objects = []
        for obj in self._objects.values():
            pos = obj.get_position(at_time)
            if pos is not None:
                objects.append(ObservedObject(obj.id, Observation(at_time, pos)))
        return objects

    def observe_object(self, obs: ObservedObject) -> None:
        if obs.id not in self._objects:
            self._objects[obs.id] = ObjectPositionModel(obs.id)
        self._objects[obs.id].observe(obs.obs.coord, obs.obs.time)

    def find_closest_object(self, ref_coord: Coord, at_time: datetime) -> Optional[Tuple[ObservedObject, float]]:
        '''Finds closest object to ref_coord by euclidean distance in coord space.'''
        distance = float('inf')
        closest_obj = None
        closest_obj_pos = None
        for obj in self._objects.values():
            obj_position = obj.get_position(at_time)
            cur_distance = distance_m(ref_coord, obj_position)
            if cur_distance < distance:
                distance = cur_distance
                closest_obj = obj
                closest_obj_pos = obj_position
        
        if closest_obj is None:
            return None, None
        
        return (ObservedObject(closest_obj.id, Observation(at_time, closest_obj_pos)), distance)
    
    def expire_objects(self, expiration_age_s: float) -> None:
        current_time = time.time()
        for obj_id in list(self._objects.keys()):
            if self._objects[obj_id].last_updated() + expiration_age_s < current_time:
                del self._objects[obj_id]
    

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

        if delta_t == 0:
            return 0, 0

        speed_lat = delta_lat / delta_t
        speed_lon = delta_lon / delta_t

        return speed_lat, speed_lon

    def last_updated(self) -> float:
        return self._last_update_ts