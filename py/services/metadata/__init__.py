from pydantic import BaseModel, Field
from typing import List, Callable, Dict, Any, Optional

class MergePlan(BaseModel):
    # id: str
    # writer_id: str
    # layer: str
    # database: str
    table: str
    from_paths: List[str]
    to: str
    iteration: int

# type Table struct {
#     Database      string
# Name          string
# Path          string
# Engine        string
# OrderBy       []string
# PartitionBy   func(map[string]data_types.IColumn) ([]PartitionDesc, error)
# AutoTimestamp bool
# Index         metadata.TableIndex
# }

class Table(BaseModel):
    # database: str
    # name: str
    # path: str
    # engine: str
    order_by: List[str]
    # partition_by: Optional[Callable[[Dict[str, IColumn]], List[PartitionDesc]]] = Field(alias="PartitionBy")
    auto_timestamp: bool = False
    # index: TableIndex

    class Config:
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
