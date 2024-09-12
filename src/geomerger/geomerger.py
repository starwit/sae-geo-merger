import logging
import time
from collections import defaultdict
from statistics import fmean
from typing import Any, Dict, List, Tuple
import datetime as dt

from prometheus_client import Counter, Histogram, Summary
from visionapi.messages_pb2 import Detection, SaeMessage

from .config import LogLevel, MergingConfig
from .geo import Coord, distance_m
from .mapper import Mapper
from .mapper import MapperEntry as ME
from .mapper import MapperError
from .model import AreaModel, ObservedObject, CameraAreaObservation, Observation

logging.basicConfig(format='%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s')
logger = logging.getLogger(__name__)

GET_DURATION = Histogram('geo_merger_get_duration', 'The time it takes to deserialize the proto until returning the tranformed result as a serialized proto',
                         buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25))
OBJECT_COUNTER = Counter('geo_merger_object_counter', 'How many detections have been processed')
PROTO_SERIALIZATION_DURATION = Summary('geo_merger_proto_serialization_duration', 'The time it takes to create a serialized output proto')
PROTO_DESERIALIZATION_DURATION = Summary('geo_merger_proto_deserialization_duration', 'The time it takes to deserialize an input proto')

def sae_message_to_model(msg: SaeMessage) -> CameraAreaObservation:
    datetime = dt.datetime.fromtimestamp(msg.frame.timestamp_utc_ms / 1000)
    id = msg.frame.source_id

    observations = []
    for det in msg.detections:
        position = Observation(datetime, Coord(det.geo_coordinate.latitude, det.geo_coordinate.longitude))
        observations.append(ObservedObject(det.object_id, position))

    cam_obs = CameraAreaObservation(id, observations)

    return cam_obs


class GeoMerger:
    def __init__(self, config: MergingConfig, log_level: LogLevel) -> None:
        logger.setLevel(log_level.value)
        self._config = config
        self._area_model = AreaModel(config.max_distance_m)
        self._mapper = Mapper()

    def __call__(self, input_proto) -> Any:
        return self.get(input_proto)
    
    @GET_DURATION.time()
    def get(self, input_proto: bytes = None) -> List[Tuple[str, bytes]]:
        input_msg = None
        if input_proto is not None:
            input_msg = self._unpack_proto(input_proto)

        # 1. Feed input to model
        if input_msg is not None:
            self._area_model.observe(sae_message_to_model(input_msg))

        self._area_model.expire_objects(expiration_age_s=0.5)

        self._update_mappings()

        out_msg = self._create_output_message()

        return [(self._config.output_stream_id, self._pack_proto(out_msg))]

    def _update_mappings(self):
        # 1. Get all objects from model 
        # objects_by_cam = self._area_model.get_all_observed_objects()

        # 2. Run algorithm to identify closest object (from other cameras) for each object
        # TODO If any pairing has more than two entries (i.e. more than two cameras overlap in the same area) log warning and skip(?)
        current_model_time = self._area_model.current_time()
        clusters = self._area_model.find_object_clusters(current_model_time)

        print('\n--- update_mappings ---\n')
        print(f'ts={current_model_time}')
        for c in clusters:
            if len(c) == 2:
                print(f'{c[1].distance:.2f}', c)
            else:
                print(c)

        # 4. Save found mappings into mapper
        # Question: do we even need a mapper now? Yes, we need the mapper for stable primary ids.
        # Maybe also save the clusters? (and use them for later reference / to implement some form of dampening, averaging over time)
        for c in clusters:
            match1 = ME(c[0].camera_id, c[0].obj.id)
            match2 = ME(c[1].camera_id, c[1].obj.id)

            match (
                self._mapper.is_primary(match1),
                self._mapper.is_secondary(match1),
                self._mapper.is_primary(match2),
                self._mapper.is_secondary(match2),
            ):
                case (False, False, False, False) | (True, False, False, False):
                    # both are new | match1 is already primary and match2 is new
                    self._mapper.map_secondary(match2, match1)
                case (False, False, True, False):
                    # match2 is already primary and match2 is new
                    self._mapper.map_secondary(match1, match2)
                case (False, True, False, False):
                    # match1 is secondary and match2 is new
                    self._mapper.remap_secondary(match1, match2)
                case (False, False, False, True):
                    # match2 is secondary and match1 is new
                    self._mapper.remap_secondary(match2, match1)
                case (True, False, False, True):
                    # match1 is primary with match2 secondary (the standard case, then check if mapping is correct)
                    if not self._mapper.is_secondary_for(match2, match1):
                        self._mapper.remap_secondary(match2, match1)
                case (False, True, True, False):
                    # match2 is primary with match1 secondary (the standard case, then check if mapping is correct)
                    if not self._mapper.is_secondary_for(match1, match2):
                        self._mapper.remap_secondary(match1, match2)
                case state:
                    logger.error(f'This should not happen! Please debug. State: {state}, match1: {match1}, match2: {match2}')                

        # 5. Prune pairings from mapper that do not fulfill mapping criteria anymore (distance only at first / no state) -> it is probably okay if mapper expires old mappings automatically
        # TODO Call expiry method here explicitly for readability?

    def _create_output_message(self) -> SaeMessage:
        # 1. Create a new output message with all known (not expired) objects and their current (interpolated and merged by avg) positions
        sae_msg = SaeMessage()
        sae_msg.frame.source_id = self._config.output_stream_id

        # Iterate over all objects (we do not need to know about the camera by this point anymore)
        # If encountered object is secondary, skip
        # If encountered object is primary, fetch secondary/ies and calculated position average

        objects = self._area_model.get_all_observed_objects()
        for obj in objects:
            
            det = Detection()
            det.class_id = 0
            det.confidence = 1.0
            det.object_id = obj.id
            det.geo_coordinate.latitude = obj.obs.coord.lat
            det.geo_coordinate.longitude = obj.obs.coord.lon
            sae_msg.detections.append(det)

        return sae_msg
        
    @PROTO_DESERIALIZATION_DURATION.time()
    def _unpack_proto(self, sae_message_bytes):
        sae_msg = SaeMessage()
        sae_msg.ParseFromString(sae_message_bytes)

        return sae_msg
    
    @PROTO_SERIALIZATION_DURATION.time()
    def _pack_proto(self, sae_msg: SaeMessage):
        return sae_msg.SerializeToString()