from geomerger.buffer import MessageBuffer
from visionapi.messages_pb2 import SaeMessage

def test_is_healthy():
    testee = MessageBuffer(target_window_size_ms=1000)
    
    testee.append(create_msg(10000))

    assert testee.is_healthy() == False

    testee.append(create_msg(11000))
    
    assert testee.is_healthy() == True

def test_pop_slice():
    testee = MessageBuffer(target_window_size_ms=1000)

    # TS interval [10000-11000]
    testee.append(create_msg(10000))
    testee.append(create_msg(10100))
    testee.append(create_msg(10200))
    testee.append(create_msg(10300))
    testee.append(create_msg(10400))
    testee.append(create_msg(10500))
    testee.append(create_msg(10600))
    testee.append(create_msg(10700))
    testee.append(create_msg(10800))
    testee.append(create_msg(10900))
    testee.append(create_msg(11000))

    assert len(testee.pop_slice(200)) == 0

    testee.append(create_msg(11100))
    testee.append(create_msg(11200))

    assert len(testee.pop_slice(400)) == 0

    slice = testee.pop_slice(200)
    assert len(slice) == 3
    assert slice[0].frame.timestamp_utc_ms == 10000
    assert slice[1].frame.timestamp_utc_ms == 10100
    assert slice[2].frame.timestamp_utc_ms == 10200

def test_pop_slice_empty():
    testee = MessageBuffer(target_window_size_ms=1000)
    assert len(testee.pop_slice(min_slice_length_ms=100)) == 0

def create_msg(timestamp_ms: int) -> SaeMessage:
    msg = SaeMessage()
    msg.frame.timestamp_utc_ms = timestamp_ms
    return msg