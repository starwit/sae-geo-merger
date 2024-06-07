import logging
import signal
import threading
import time
from typing import List, Tuple

from prometheus_client import Counter, Histogram, start_http_server
from visionlib.pipeline.consumer import RedisConsumer
from visionlib.pipeline.publisher import RedisPublisher

from .config import GeoMergerConfig
from .geomerger import GeoMerger

logger = logging.getLogger(__name__)

REDIS_PUBLISH_DURATION = Histogram('geo_merger_redis_publish_duration', 'The time it takes to push a message onto the Redis stream',
                                   buckets=(0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.25))
FRAME_COUNTER = Counter('geo_merger_frame_counter', 'How many frames have been consumed from the Redis input stream')

def run_stage():

    stop_event = threading.Event()

    # Register signal handlers
    def sig_handler(signum, _):
        signame = signal.Signals(signum).name
        print(f'Caught signal {signame} ({signum}). Exiting...')
        stop_event.set()

    signal.signal(signal.SIGTERM, sig_handler)
    signal.signal(signal.SIGINT, sig_handler)

    # Load config from settings.yaml / env vars
    CONFIG = GeoMergerConfig()

    logger.setLevel(CONFIG.log_level.value)

    logger.info(f'Starting prometheus metrics endpoint on port {CONFIG.prometheus_port}')

    start_http_server(CONFIG.prometheus_port)

    logger.info(f'Starting geo mapper stage. Config: {CONFIG.model_dump_json(indent=2)}')

    geo_merger = GeoMerger(CONFIG.merging_config, CONFIG.log_level)

    consume = RedisConsumer(CONFIG.redis.host, CONFIG.redis.port, 
                            stream_keys=[f'{CONFIG.redis.input_stream_prefix}:{stream_id}' for stream_id in CONFIG.merging_config.input_stream_ids],
                            block=500)
    publish = RedisPublisher(CONFIG.redis.host, CONFIG.redis.port)
    
    with consume, publish:
        for _, proto_data in consume():
            if stop_event.is_set():
                break

            if proto_data is None:
                time.sleep(0.01)

            if proto_data is not None:
                FRAME_COUNTER.inc()

            output_records: List[Tuple[str, bytes]] = geo_merger.get(proto_data)

            for stream_id, output_proto_data in output_records:
                with REDIS_PUBLISH_DURATION.time():
                    publish(f'{CONFIG.redis.output_stream_prefix}:{stream_id}', output_proto_data)