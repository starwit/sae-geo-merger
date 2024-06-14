from typing import List, NamedTuple, Tuple

import numpy as np
import pytest
from visionapi.messages_pb2 import Detection, SaeMessage

from geomerger.config import LogLevel, MergingConfig
from geomerger.geo import Coord, m_per_deg_lat, m_per_deg_lon
from geomerger.geomerger import GeoMerger

LAT_REF = 50
LON_REF = 12
M_PER_DEG_LAT = m_per_deg_lat(LAT_REF)
M_PER_DEG_LON = m_per_deg_lon(LAT_REF)

class XYCoord(NamedTuple):
    x: float
    y: float

class DetectionStream(NamedTuple):
    time_range: List[int]
    object_id: str


@pytest.fixture
def testee():
    config = MergingConfig(
        input_stream_ids=['stream1', 'stream2'],
        max_distance_m=2,
        merging_window_ms=1000,
        target_mps=5,
        output_stream_id='output',
    )
    testee = GeoMerger(config, log_level=LogLevel.INFO)
    return testee

def test_two_streams_full_overlap(testee):
    pass


def create_detection_trajectory(start_coord: XYCoord, stop_coord: XYCoord, start_time_ms: int, stop_time_ms: int, object_id: bytes, num_points: int) -> List[Tuple[int, Detection]]:
    coords = create_linear_geotrajectory(start_coord, stop_coord, num_points)
    time_range = np.linspace(start_time_ms, stop_time_ms, num_points)
    output: List[Tuple[int, Detection]] = []
    for c, ts in zip(coords, time_range):
        output.append((ts, create_det(oid=object_id, lat=c.lat, lon=c.lon)))
    return output

def create_time_range(start_ms: int, stop_ms: int, num_points: int) -> List[int]:
    return np.linspace(start_ms, stop_ms, num_points)

def create_linear_geotrajectory(start: XYCoord, stop: XYCoord, num_points: int) -> List[Coord]:
    '''XYCoord.x/y is meant to be in meters. x=0 is LON_REF, y=0 is LAT_REF'''
    start_geo = Coord(lat=LAT_REF + start.y / M_PER_DEG_LAT, lon=LON_REF + start.x / M_PER_DEG_LON)
    stop_geo = Coord(lat=LAT_REF + stop.y / M_PER_DEG_LAT, lon=LON_REF + stop.x / M_PER_DEG_LON)
    points = np.linspace((start_geo.lat, start_geo.lon), (stop_geo.lat, stop_geo.lon), num_points)
    return [Coord(lat=p[0], lon=p[1]) for p in points]

def create_msg(ts_ms: int, dets: List[Detection], source_id: str) -> SaeMessage:
    msg = SaeMessage()
    msg.frame.timestamp_utc_ms = ts_ms
    msg.frame.source_id = source_id
    msg.detections.extend(dets)
    return msg

def create_det(oid: bytes, lat: float, lon: float) -> Detection:
    det = Detection()
    det.geo_coordinate.latitude = lat
    det.geo_coordinate.longitude = lon
    det.class_id = 1
    det.object_id = oid
    return det