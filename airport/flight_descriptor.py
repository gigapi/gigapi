from dataclasses import dataclass
from typing import Literal

import pyarrow.flight as flight

ObjectTypeName = Literal["table"]


def type_name_from_str(type_name: str) -> ObjectTypeName:
    """Convert a string to an ObjectTypeName."""
    if type_name == "table":
        return "table"
    raise ValueError(f"Invalid type name: {type_name}")


@dataclass
class FlightDescriptorParts:
    """Fields encoded in the Flight descriptor."""

    catalog_name: str
    schema_name: str
    type: ObjectTypeName
    name: str

    def pack(self) -> flight.FlightDescriptor:
        """Pack into a FlightDescriptor."""
        return flight.FlightDescriptor.for_path(f"{self.catalog_name}/{self.schema_name}/{self.type}/{self.name}")

    @staticmethod
    def unpack(descriptor: flight.FlightDescriptor) -> "FlightDescriptorParts":
        """Unpack a FlightDescriptor into DescriptorParts."""
        if descriptor.descriptor_type != flight.DescriptorType.PATH or len(descriptor.path) != 1:
            raise flight.FlightServerError("Descriptor must be a single-path PATH type.")

        path = descriptor.path[0].decode("utf-8")
        parts = path.split("/")
        if len(parts) != 4:
            raise flight.FlightServerError(f"Invalid descriptor path: {path}")

        return FlightDescriptorParts(
            catalog_name=parts[0],
            schema_name=parts[1],
            type=type_name_from_str(parts[2]),
            name=parts[3],
        )
