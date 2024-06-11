import logging
import time
from collections import defaultdict, deque
from statistics import fmean
from typing import Any, Deque, Dict, List, Optional, Tuple

from prometheus_client import Counter, Histogram, Summary
from visionapi.messages_pb2 import Detection, SaeMessage

from .buffer import MessageBuffer
from .config import LogLevel, MergingConfig
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

        self._buffer = MessageBuffer(target_window_size_ms=config.merging_window_ms)
        self._last_emission = 0
        self._mapper = Mapper()

    def __call__(self, input_proto) -> Any:
        return self.get(input_proto)
    
    @GET_DURATION.time()
    def get(self, input_proto: bytes = None) -> List[Tuple[str, bytes]]:
        input_msg = None
        if input_proto is not None:
            input_msg = self._unpack_proto(input_proto)

        if input_msg is not None:
            self._buffer.append(input_msg)

        if self._buffer.is_healthy():
            for buffer_msg in self._buffer:
                for buffer_det in buffer_msg.detections:
                    closest_det = self._find_closest_detection(buffer_msg, buffer_det)
                    buffer_id = buffer_det.object_id
                    
                    if closest_det is not None:
                        closest_id = closest_det.object_id

                        match (
                            self._mapper.is_primary(closest_id),
                            self._mapper.is_secondary(closest_id),
                            self._mapper.is_primary(buffer_id),
                            self._mapper.is_secondary(buffer_id),
                        ):
                            case (False, False, False, False) | (True, False, False, False):
                                # Both ids are new or the input is new
                                self._mapper.map_secondary(buffer_id, closest_id)
                                logger.info(f'Mapped {buffer_id.hex()[:4]} to {closest_id.hex()[:4]}')
                            case (False, True, False, False):
                                primary = self._mapper.get_primary(closest_id)
                                if not primary == buffer_id:
                                    self._mapper.map_secondary(buffer_id, primary)
                                    logger.info(f'Mapped {buffer_id.hex()[:4]} to {primary.hex()[:4]}')
                            case (False, True, True, False):
                                primary = self._mapper.get_primary(closest_id)
                                if not primary == buffer_id:
                                    self._mapper.demote_primary(buffer_id, new_primary=primary, migrate_children=True)
                                    logger.info(f'Demoted {buffer_id.hex()[:4]} to secondary of {primary.hex()[:4]}')
                            case (True, False, True, False):
                                self._mapper.demote_primary(buffer_id, new_primary=closest_id, migrate_children=True)
                                logger.info(f'Demoted {buffer_id.hex()[:4]} to secondary of {closest_id.hex()[:4]}')
                            case (True, False, False, True):
                                if not self._mapper.is_secondary_for(buffer_id, primary=closest_id):
                                    self._mapper.remap_secondary(buffer_id, closest_id)
                                    logger.info(f'Remapped {buffer_id} to {closest_id}')
                            case state:
                                logger.error(f'This should not happen! Please debug. State: {state}')

        out_buffer = self._buffer.pop_slice(min_slice_length_ms=(1 / self._config.target_mps))
        if len(out_buffer) == 0:
            return []

        # Apply active mappings to all outgoing messages
        out_buffer = self._apply_mappings(out_buffer)

        # Remove all duplicate detections but the first
        out_msg = self._merge_messages(out_buffer)

        print(f'len buf: {len(self._buffer)}; len out: {len(out_buffer)}; since last: {round(time.time() - self._last_emission, 3)}')

        self._last_emission = time.time()
        return [(self._config.output_stream_id, self._pack_proto(out_msg))]
        
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