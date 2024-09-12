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
from .mapper import ExpiringMapper
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
        self._area_model = AreaModel()
        self._mapper = ExpiringMapper(entry_expiration_age_s=config.expire_ids_after_s)

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

        # 2. Find objects from different cameras that are closer than a certain threshold (and have been for some time)
        # This needs to be somewhat efficient, b/c the naive implementation is exponential with num cameras and objects
        self._update_mappings()

        self._area_model.expire_objects(expiration_age_s=0.5)

        out_msg = self._create_output_message()

        return [(self._config.output_stream_id, self._pack_proto(out_msg))]
            
    def _update_mappings(self):
        pass
        # 1. Get all objects from model 
        # 2. Run algorithm to identify closest object (from other cameras) for each object
        # 2b. If any pairing has more than two entries (i.e. more than two cameras overlap in the same area) log warning and skip
        # 3. Check pairings if merging criteria are met (start with distance only)
        # 4. Save found mappings into mapper
        # 5. Prune pairings from mapper that do not fulfill mapping criteria anymore (distance only at first / no state)

    def _create_output_message(self) -> SaeMessage:
        # 1. Create a new output message with all known (not expired) objects and their current (interpolated and merged by avg) positions
        sae_msg = SaeMessage()
        sae_msg.frame.source_id = self._config.output_stream_id

        objects = self._area_model.get_all_observed_objects()
        # TODO Here we need to do the merging according to saved mappings
        for obj in objects:
            det = Detection()
            det.class_id = 0
            det.confidence = 1.0
            det.object_id = obj.id
            det.geo_coordinate.latitude = obj.obs.coord.lat
            det.geo_coordinate.longitude = obj.obs.coord.lon
            sae_msg.detections.append(det)

        return sae_msg
        
    def _find_match(self, input_msg: SaeMessage, input_det: Detection) -> Tuple[Detection, SaeMessage]:
        closest_det: Detection = None
        closest_msg: SaeMessage = None
        closest_distance: float = 999999
        for msg in self._buffer:
            # Do not match detections of the same source / camera
            # TODO Prevent that from happening in the first place (-> performance)
            if msg.frame.source_id == input_msg.frame.source_id:
                continue
            t_dist = abs(input_msg.frame.timestamp_utc_ms - msg.frame.timestamp_utc_ms)
            for det in msg.detections:
                # Do not match detections of different classes
                # TODO Treat some classes as equal (e.g. trucks and cars)
                if det.class_id != input_det.class_id:
                    continue
                s_dist = self._get_spatial_distance(input_det, det)
                dist = s_dist * t_dist
                if self._is_similar(input_det, det) and s_dist < self._config.max_distance_m:
                    if closest_distance > dist:
                        closest_distance = dist
                        closest_det = det
                        closest_msg = msg
        return closest_det, closest_msg
    
    def _is_similar(self, det1: Detection, det2: Detection):
        return all((
            det1.class_id == det2.class_id,
        ))
    
    def _get_spatial_distance(self, det1: Detection, det2: Detection) -> float:
        return distance_m(
            Coord(det1.geo_coordinate.latitude, det1.geo_coordinate.longitude), 
            Coord(det2.geo_coordinate.latitude, det2.geo_coordinate.longitude)
        )

    def _apply_mappings(self, messages: List[SaeMessage]) -> List[SaeMessage]:
        for msg in messages:
            for det in msg.detections:
                if self._mapper.is_secondary(ME(msg.frame.source_id, det.object_id)):
                    primary = self._mapper.get_primary(ME(msg.frame.source_id, det.object_id))
                    det.object_id = primary.object_id
        return messages
    
    def _merge_messages(self, messages: List[SaeMessage]) -> SaeMessage:
        '''Merges all given messages into one (dropping the frames)'''
        if len(messages) == 0:
            return None
        
        out_msg = SaeMessage()
        out_msg.frame.shape.CopyFrom(messages[0].frame.shape)
        detections = []
        earliest_timestamp = time.time_ns() // 1_000_000

        for msg in messages:
            detections.extend(msg.detections)
            if msg.frame.timestamp_utc_ms < earliest_timestamp:
                earliest_timestamp = msg.frame.timestamp_utc_ms

        detections = self._aggregate_duplicate_detections(detections)
        out_msg.detections.extend(detections)
        out_msg.frame.timestamp_utc_ms = earliest_timestamp
        
        return out_msg

    def _aggregate_duplicate_detections(self, detections: List[Detection]) -> List[Detection]:
        aggregated_dets = []

        dets_by_id: Dict[bytes, List[Detection]] = defaultdict(list)
        for det in detections:
            dets_by_id[det.object_id].append(det)

        for dets in dets_by_id.values():
            if len(dets) == 0:
                continue
            agg_det = Detection()
            agg_det.class_id = dets[0].class_id
            agg_det.object_id = dets[0].object_id
            agg_det.geo_coordinate.latitude = fmean([d.geo_coordinate.latitude for d in dets])
            agg_det.geo_coordinate.longitude = fmean([d.geo_coordinate.longitude for d in dets])
            agg_det.confidence = fmean([d.confidence for d in dets])
            aggregated_dets.append(agg_det)

        return aggregated_dets

    @PROTO_DESERIALIZATION_DURATION.time()
    def _unpack_proto(self, sae_message_bytes):
        sae_msg = SaeMessage()
        sae_msg.ParseFromString(sae_message_bytes)

        return sae_msg
    
    @PROTO_SERIALIZATION_DURATION.time()
    def _pack_proto(self, sae_msg: SaeMessage):
        return sae_msg.SerializeToString()