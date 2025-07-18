from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class MetadataConfiguration(BaseModel):
    type: str = Field("ducklake", description="Type of metadata storage (json or redis)")
    url: str = Field("", description="Redis URL for metadata storage")

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
conf = GigapiConfiguration()
http = HTTPConfiguration()
settings.gigapi = conf
settings.http = http
