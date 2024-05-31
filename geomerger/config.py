from typing import List

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing_extensions import Annotated
from visionlib.pipeline.settings import LogLevel, YamlConfigSettingsSource


class RedisConfig(BaseModel):
    host: str = 'localhost'
    port: Annotated[int, Field(ge=1, le=65536)] = 6379
    stream_id: str = 'stream1'
    input_stream_prefix: str = 'objecttracker'
    output_stream_prefix: str = 'geomerger'

class MergingConfig(BaseModel):
    max_distance_m: float
    max_time_drift_s: float
    stream_ids: Annotated[List[str], Field(min_length=1)]

class GeoMergerConfig(BaseSettings):
    log_level: LogLevel = LogLevel.WARNING
    redis: RedisConfig = RedisConfig()
    prometheus_port: Annotated[int, Field(ge=1024, le=65536)] = 8000
    merging_config: MergingConfig

    model_config = SettingsConfigDict(env_nested_delimiter='__')

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings):
        return (init_settings, env_settings, YamlConfigSettingsSource(settings_cls), file_secret_settings)