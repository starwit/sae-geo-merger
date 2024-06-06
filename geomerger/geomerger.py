import logging
import math
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, NamedTuple, Optional, Tuple
from uuid import uuid4

from prometheus_client import Counter, Histogram, Summary
from visionapi.messages_pb2 import (BoundingBox, Detection, GeoCoordinate,
                                    SaeMessage)

from .config import GeoMergerConfig, LogLevel, MergingConfig
from .geo import Coord, distance_m
from .mapper import Mapper

logging.basicConfig(format='%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s')
logger = logging.getLogger(__name__)

GET_DURATION = Histogram('geo_merger_get_duration', 'The time it takes to deserialize the proto until returning the tranformed result as a serialized proto',
                         buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25))
OBJECT_COUNTER = Counter('geo_merger_object_counter', 'How many detections have been processed')
PROTO_SERIALIZATION_DURATION = Summary('geo_merger_proto_serialization_duration', 'The time it takes to create a serialized output proto')
PROTO_DESERIALIZATION_DURATION = Summary('geo_merger_proto_deserialization_duration', 'The time it takes to deserialize an input proto')

class GeoMerger:
    def __init__(self, config: MergingConfig, log_level: LogLevel) -> None:
        logger.setLevel(log_level.value)
        self._config = config

        self._buffer: Deque[SaeMessage] = deque(maxlen=100)
        self._estimated_stream_head = 0
        self._last_update = 0
        self._mapper = Mapper()

    def __call__(self, input_proto) -> Any:
        return self.get(input_proto)
    
    @GET_DURATION.time()
    def get(self, input_proto: bytes = None) -> List[Tuple[str, bytes]]:
        input_msg = None
        if input_proto is not None:
            input_msg = self._unpack_proto(input_proto)

        # Update stream head (extrapolate, if there was no message given)
        if input_msg is not None:
            self._estimated_stream_head = input_msg.frame.timestamp_utc_ms
        else:
            elapsed_time = time.time_ns() // 1_000_000 - self._last_update
            self._estimated_stream_head += elapsed_time
        self._last_update = time.time_ns() // 1_000_000

        if input_msg is not None:
            for input_det in input_msg.detections:
                closest_det = self._find_closest_detection(input_msg, input_det)
                
                if closest_det is not None:
                    if self._mapper.is_primary(closest_det.object_id) and self._mapper.get_primary(input_det.object_id) is None:
                        self._mapper.map_secondary(input_det.object_id, closest_det.object_id)
                        logger.info(f'Mapped {input_det.object_id.hex()[:4]} to {closest_det.object_id.hex()[:4]}')
                    elif self._mapper.is_secondary(closest_det.object_id):
                        primary = self._mapper.get_primary(closest_det.object_id)
                        if not primary == input_det.object_id:
                            self._mapper.map_secondary(input_det.object_id, primary)
                            logger.info(f'Mapped {input_det.object_id.hex()[:4]} to {primary.hex()[:4]}')
                else:
                    if not self._mapper.is_known(input_det.object_id):
                        self._mapper.add_primary(input_det.object_id)
                        logger.info(f'Added primary {input_det.object_id.hex()[:4]}')
            
            self._buffer.append(input_msg)

        out_buffer = self._retrieve_expired_messages()

        # Apply active mappings to all outgoing messages
        out_buffer = self._apply_mappings(out_buffer)

        # Remove all duplicate detections but the first
        out_buffer = self._remove_duplicate_detections(out_buffer)

        return [(self._config.output_stream_id, self._pack_proto(out_msg)) for out_msg in out_buffer]
    
    def _retrieve_expired_messages(self) -> List[SaeMessage]:
        expired = []
        while len(self._buffer) > 0 and (self._buffer[0].frame.timestamp_utc_ms < self._estimated_stream_head - self._config.merging_window_ms):
            expired.append(self._buffer.popleft())
        return expired
    
    def _find_closest_detection(self, input_msg: SaeMessage, input_det: Detection) -> Optional[Detection]:
        closest_det: Detection = None
        closest_distance: float = 999999
        for msg in self._buffer:
            if msg.frame.source_id == input_msg.frame.source_id:
                continue
            t_dist = abs(input_msg.frame.timestamp_utc_ms - msg.frame.timestamp_utc_ms)
            for det in msg.detections:
                s_dist = self._get_spatial_distance(input_det, det)
                dist = s_dist * t_dist
                if self._is_similar(input_det, det) and s_dist < self._config.max_distance_m:
                    if closest_distance > dist:
                        closest_distance = dist
                        closest_det = det
        return closest_det
    
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
                if self._mapper.is_secondary(det.object_id):
                    primary = self._mapper.get_primary(det.object_id)
                    det.object_id = primary
        return messages
    
    def _remove_duplicate_detections(self, messages: List[SaeMessage]) -> List[SaeMessage]:
        already_seen_ids: List[bytes] = []
        for msg in messages:
            idxs_to_remove: List[int] = []
            for idx, det in enumerate(msg.detections):
                if det.object_id in already_seen_ids:
                    idxs_to_remove
                else:
                    already_seen_ids.append(det.object_id)
            for idx in reversed(idxs_to_remove):
                del msg.detections[idx]
        return messages        

    @PROTO_DESERIALIZATION_DURATION.time()
    def _unpack_proto(self, sae_message_bytes):
        sae_msg = SaeMessage()
        sae_msg.ParseFromString(sae_message_bytes)

        return sae_msg
    
    @PROTO_SERIALIZATION_DURATION.time()
    def _pack_proto(self, sae_msg: SaeMessage):
        return sae_msg.SerializeToString()