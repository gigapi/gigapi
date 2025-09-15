from pydantic import BaseModel, Field
from typing import List, Optional
from enum import Enum

class LayerType(Enum):
    FILE = "fs"
    S3 = "s3"

class LayerConfig(BaseModel):
    name: str
    type: LayerType
    is_global: bool
    url: str
    ttl_sec: int



class Config(BaseModel):
    root_folder: str
    location: str
    layer_configuration: List[LayerConfig]

_config: Optional[Config] = None

def set_config(conf: Config) -> None:
    global _config
    _config = conf

def config() -> Optional[Config]:
    return _config
