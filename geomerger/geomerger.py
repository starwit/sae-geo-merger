import logging
import math
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, NamedTuple, Tuple

from prometheus_client import Counter, Histogram, Summary
from visionapi.messages_pb2 import (BoundingBox, Detection, GeoCoordinate,
                                    SaeMessage)

from .config import GeoMergerConfig
from .geo import distance_m, Coord

logging.basicConfig(format='%(asctime)s %(name)-15s %(levelname)-8s %(processName)-10s %(message)s')
logger = logging.getLogger(__name__)

GET_DURATION = Histogram('geo_merger_get_duration', 'The time it takes to deserialize the proto until returning the tranformed result as a serialized proto',
                         buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25))
OBJECT_COUNTER = Counter('geo_merger_object_counter', 'How many detections have been processed')
PROTO_SERIALIZATION_DURATION = Summary('geo_merger_proto_serialization_duration', 'The time it takes to create a serialized output proto')
PROTO_DESERIALIZATION_DURATION = Summary('geo_merger_proto_deserialization_duration', 'The time it takes to deserialize an input proto')

class GeoMerger:
    def __init__(self, config: GeoMergerConfig) -> None:
        self._config = config
        logger.setLevel(self._config.log_level.value)

        self._buffers_by_stream : Dict[str, Deque[SaeMessage]] = defaultdict(lambda: deque(maxlen=100))
        self._estimated_stream_head = 0
        self._last_update = 0

    def __call__(self, input_proto) -> Any:
        return self.get(input_proto)
    
    @GET_DURATION.time()
    def get(self, input_proto: bytes = None) -> List[Tuple[str, bytes]]:
        if input_proto is not None:
            sae_msg = self._unpack_proto(input_proto)

        # Update stream head (extrapolate, if there was no message given)
        if sae_msg is not None:
            self._estimated_stream_head = sae_msg.frame.timestamp_utc_ms
        else:
            elapsed_time = time.time() - self._last_update
            self._estimated_stream_head += elapsed_time
        self._last_update = time.time()

        out_buffer = self._get_expired_messages()

        if sae_msg is not None:
            # Check all buffers for similar detections
            closest_det: Detection = None
            closest_distance: float = 999999
            for stream_id, buffer in self._buffers_by_stream.items():
                if stream_id == sae_msg.frame.source_id:
                    continue
                for input_det in sae_msg.detections:
                    for msg in buffer:
                        for det in msg.detections:
                            dist = self._get_distance(input_det.geo_coordinate, det.geo_coordinate)
                            if self._is_similar(input_det, det) and dist < self._config.merging_config.max_distance_m:
                                if closest_distance > dist:
                                    closest_distance = dist
                                    closest_det = det
            
            # Here, we have found (albeit in a terribly inefficient way) the closest detection in our buffers meeting all similarity requirements
            if closest_det is not None:
                # Assume that the existing detection is 
                closest_det.object_id
                pass


        return [(out_msg.frame.source_id, self._pack_proto(sae_msg)) for out_msg in out_buffer]
    
    def _get_expired_messages(self) -> List[SaeMessage]:
        expired = []
        for buffer in self._buffers_by_stream.values():
            while len(buffer) > 0 and (buffer[0].frame.timestamp_utc_ms < (self._estimated_stream_head - self._config.merging_config.max_time_drift_s)):
                expired.append(buffer.popleft())
        return expired
    
    def _is_similar(self, det1: Detection, det2: Detection):
        return all(
            det1.class_id == det2.class_id,
        )
    
    def _get_distance(self, coord1: GeoCoordinate, coord2: GeoCoordinate):
        return distance_m(Coord(coord1.latitude, coord1.longitude), Coord(coord2.latitude, coord2.longitude))

    @PROTO_DESERIALIZATION_DURATION.time()
    def _unpack_proto(self, sae_message_bytes):
        sae_msg = SaeMessage()
        sae_msg.ParseFromString(sae_message_bytes)

        return sae_msg
    
    @PROTO_SERIALIZATION_DURATION.time()
    def _pack_proto(self, sae_msg: SaeMessage):
        return sae_msg.SerializeToString()