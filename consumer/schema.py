import pyarrow as pa

FIELDS = [
    "message_type",
    "transmission_type",
    "session_id",
    "aircraft_id",
    "hex_ident",
    "flight_id",
    "date_message_generated",
    "time_message_generated",
    "date_message_logged",
    "time_message_logged",
    "callsign",
    "altitude",
    "ground_speed",
    "track",
    "latitude",
    "longitude",
    "vertical_rate",
    "squawk",
    "alert",
    "emergency",
    "spi",
    "is_on_ground",
]

SCHEMA = pa.schema(
    [(f, pa.string()) for f in FIELDS]
    + [("kafka_timestamp", pa.timestamp("ms", tz="UTC"))]
)
