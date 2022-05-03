"""TVSquared tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_tvsquared.streams import (
    InventoryStream,
    BrandsStream,
    SpotsStream,
    SpotColumnsStream,
    SpotsArrayStream,
)
STREAM_TYPES = [
    InventoryStream,
    BrandsStream,
    SpotsStream,
    SpotColumnsStream,
    SpotsArrayStream,
]


class TapTVSquared(Tap):
    """TVSquared tap class."""
    name = "tap-tvsquared"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "partner_domain",
            th.StringType,
            required=True,
            description="Partner domain for your TVSquared account"
        ),
        th.Property(
            "api_number",
            th.NumberType,
            required=True,
            description="Number for your account API, i.e. https://api-xx.tvsquared.com"
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
            description="Username for authentication"
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            description="Password for authentication"
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            default="2016-01-01T00:00:00Z",
            description="Date at which to start syncing attribution"
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
