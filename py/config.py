from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional, List

class MetadataConfiguration(BaseSettings):
    type: str = Field("ducklake", description="Type of metadata storage (json or redis)")
    url: str = Field("", description="Redis URL for metadata storage")
    class Config:
        env_prefix = "GIGAPI_METADATA_"
        env_nested_delimiter = ""
        extra = "allow"

class BasicAuthConfiguration(BaseModel):
    username: str = Field("", description="Username for basic authentication")
    password: str = Field("", description="Password for basic authentication")

class FlightSqlConfiguration(BaseModel):
    port: int = Field(8082, description="Port to run flightSQL server")
    enable: bool = Field(True, description="Enable FlightSQL server")

class HTTPConfiguration(BaseSettings):
    port: int = Field(7971, description="Port to listen on")
    host: str = Field("0.0.0.0", description="Host to bind to (0.0.0.0 for all interfaces)")
    basic_auth: BasicAuthConfiguration = Field(default_factory=BasicAuthConfiguration)
    class Config:
        env_prefix = "HTTP_"
        env_nested_delimiter = ""
        extra = "allow"

class GigapiConfiguration(BaseSettings):
    root: str = Field(".", description="Root folder for all the data files")
    merge_timeout_s: int = Field(10, description="Base timeout between merges")
    save_timeout_s: float = Field(1.0, description="Timeout before saving the new data to the disk")
    no_merges: bool = Field(False, description="Disable merging")
    ui: bool = Field(True, description="Enable UI for querier")
    mode: str = Field("aio", description="Execution mode (readonly, writeonly, compaction, aio)")
    metadata: MetadataConfiguration = Field(default_factory=MetadataConfiguration)
    class Config:
        env_prefix = "GIGAPI_"
        env_nested_delimiter = ""
        extra = "allow"

class Settings(BaseSettings):
    gigapi: Optional[GigapiConfiguration] = None
    http: Optional[HTTPConfiguration] = None
    flightsql: FlightSqlConfiguration = Field(default_factory=FlightSqlConfiguration)
    loglevel: str = Field("info", description="Log level (debug, info, warn, error, fatal)")
    class Config:
        env_nested_delimiter = "_"
        extra = "allow"

settings = Settings()
settings.gigapi = GigapiConfiguration()
settings.http = HTTPConfiguration()
settings.gigapi.metadata = MetadataConfiguration()

class MergeConfiguration:
    def __init__(self, timeout_s: int, max_result_bytes: int, iteration: int):
        self._timeout_s = timeout_s
        self._max_result_bytes = max_result_bytes
        self._iteration = iteration

    def timeout_s(self) -> int:
        return self._timeout_s

    def max_result_bytes(self) -> int:
        return self._max_result_bytes

    def iteration(self) -> int:
        return self._iteration

def get_merge_configurations() -> List[MergeConfiguration]:
    return [
        MergeConfiguration(timeout_s=settings.gigapi.merge_timeout_s, max_result_bytes=100 * 1024 * 1024, iteration=1),
        MergeConfiguration(timeout_s=settings.gigapi.merge_timeout_s * 10, max_result_bytes=400 * 1024 * 1024, iteration=2),
        MergeConfiguration(timeout_s=settings.gigapi.merge_timeout_s * 100, max_result_bytes=4000 * 1024 * 1024 * 4, iteration=3),
        MergeConfiguration(timeout_s=settings.gigapi.merge_timeout_s * 420, max_result_bytes=4000 * 1024 * 1024 * 4, iteration=4),
    ]