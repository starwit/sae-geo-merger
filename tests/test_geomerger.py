from typing import Iterable

import pybase64
import pytest
from visionapi.messages_pb2 import SaeMessage
from visionlib.saedump import Event, message_splitter

from geomerger.config import LogLevel, MergingConfig
from geomerger.geomerger import GeoMerger


@pytest.fixture
def testee():
    config = MergingConfig(
        input_stream_ids=['geomapper:Monon1stStreetNB', 'geomapper:Monon1stStreetSB'],
        max_distance_m=2,
        target_mps=5,
        output_stream_id='output',
    )
    testee = GeoMerger(config, log_level=LogLevel.INFO)
    return testee

@pytest.fixture
def test_data() -> Iterable[SaeMessage]:
    with open('tests/data/test_geomerger_1.saedump', 'r') as dump_file:
        messages = message_splitter(dump_file)
        next(messages)
        proto_bytes_list = []
        for message in messages:
            event = Event.model_validate_json(message)
            proto_bytes = pybase64.standard_b64decode(event.data_b64)
            proto_bytes_list.append(proto_bytes)
    return proto_bytes_list

# TODO This test does not make a lot of sense without a useful metric establishing "success" and "failure"...
def test_geomerger(testee, test_data):
    output = []
    for proto_bytes in test_data:
        output.extend(testee.get(proto_bytes))
    assert len(output) == 189