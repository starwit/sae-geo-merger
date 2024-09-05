import logging
import time
from collections import defaultdict
from statistics import fmean
from typing import Any, Dict, List, Optional, Tuple

from prometheus_client import Counter, Histogram, Summary
from visionapi.messages_pb2 import Detection, SaeMessage

from .buffer import MessageBuffer
from .config import LogLevel, MergingConfig
from .geo import Coord, distance_m
from .mapper import ExpiringMapper
from .mapper import MapperEntry as ME
from .mapper import MapperError

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
        self._mapper = ExpiringMapper(entry_expiration_age_s=config.expire_ids_after_s)

    def __call__(self, input_proto) -> Any:
        return self.get(input_proto)
    
    @GET_DURATION.time()
    def get(self, input_proto: bytes = None) -> List[Tuple[str, bytes]]:
        input_msg = None
        if input_proto is not None:
            input_msg = self._unpack_proto(input_proto)

        if input_msg is not None:
            self._buffer.append(input_msg)

        self._update_mappings()

        out_buffer = self._buffer.pop_slice(min_slice_length_ms=(1 / self._config.target_mps))
        if len(out_buffer) == 0:
            return []

        # Apply active mappings to all outgoing messages
        out_buffer = self._apply_mappings(out_buffer)

        # Remove all duplicate detections but the first
        out_msg = self._merge_messages(out_buffer)

        # logger.debug(f'len buf: {len(self._buffer)}; len out: {len(out_buffer)}; since last: {round(time.time() - self._last_emission, 3)}')

        self._last_emission = time.time()
        return [(self._config.output_stream_id, self._pack_proto(out_msg))]
        
    def _update_mappings(self):
        try:
            if self._buffer.is_healthy():
                for buffer_msg in self._buffer:
                    for buffer_det in buffer_msg.detections:
                        match_det, match_msg = self._find_match(buffer_msg, buffer_det)
                        
                        if match_det is not None:
                            match_entry = ME(match_msg.frame.source_id, match_det.object_id)
                            buffer_entry = ME(buffer_msg.frame.source_id, buffer_det.object_id)

                            match (
                                self._mapper.is_primary(match_entry),
                                self._mapper.is_secondary(match_entry),
                                self._mapper.is_primary(buffer_entry),
                                self._mapper.is_secondary(buffer_entry),
                            ):
                                case (False, False, False, False) | (True, False, False, False):
                                    # Both ids are new or the input is new
                                    self._mapper.map_secondary(buffer_entry, match_entry)
                                    logger.info(f'Mapped {buffer_entry} to {match_entry}')
                                case (False, True, False, False):
                                    primary = self._mapper.get_primary(match_entry)
                                    if not primary == buffer_entry and not primary.source_id == buffer_entry.source_id:
                                        self._mapper.map_secondary(buffer_entry, primary)
                                        logger.info(f'Mapped {buffer_entry} to {primary}')
                                case (False, True, True, False):
                                    primary = self._mapper.get_primary(match_entry)
                                    if not primary == buffer_entry and not primary.source_id == buffer_entry.source_id:
                                        self._mapper.demote_primary(buffer_entry, new_primary=primary, migrate_children=True)
                                        logger.info(f'Demoted {buffer_entry} to secondary of {primary}')
                                case (False, False, False, True):
                                    primary = self._mapper.get_primary(buffer_entry)
                                    if not primary == buffer_entry and not primary.source_id == match_entry.source_id:
                                        self._mapper.map_secondary(match_entry, primary)
                                        logger.info(f'Mapped {match_entry} to {primary}')
                                case (True, False, True, False):
                                    self._mapper.demote_primary(buffer_entry, new_primary=match_entry, migrate_children=True)
                                    logger.info(f'Demoted {buffer_entry} to secondary of {match_entry}')
                                case (True, False, False, True):
                                    if not self._mapper.is_secondary_for(buffer_entry, primary=match_entry):
                                        self._mapper.remap_secondary(buffer_entry, match_entry)
                                        logger.info(f'Remapped {buffer_entry} to {match_entry}')
                                case state:
                                    logger.error(f'This should not happen! Please debug. State: {state}; buffer_entry: {buffer_entry}; match_entry: {match_entry}')
        except MapperError:
            logger.error(f'Illegal state encountered', exc_info=True)

        
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