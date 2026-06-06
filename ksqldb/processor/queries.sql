CREATE STREAM IF NOT EXISTS adsb_raw_stream (raw_msg VARCHAR) WITH (
    KAFKA_TOPIC = 'adsb.raw',
    VALUE_FORMAT = 'KAFKA'
);
CREATE OR REPLACE STREAM adsb_parsed WITH (
        KAFKA_TOPIC = 'adsb.parsed',
        VALUE_FORMAT = 'JSON_SR',
        PARTITIONS = 1
    ) AS
SELECT SPLIT(raw_msg, ',') [1] AS message_type,
    SPLIT(raw_msg, ',') [2] AS transmission_type,
    SPLIT(raw_msg, ',') [3] AS session_id,
    SPLIT(raw_msg, ',') [4] AS aircraft_id,
    SPLIT(raw_msg, ',') [5] AS hex_ident,
    SPLIT(raw_msg, ',') [6] AS flight_id,
    SPLIT(raw_msg, ',') [7] AS date_message_generated,
    SPLIT(raw_msg, ',') [8] AS time_message_generated,
    SPLIT(raw_msg, ',') [9] AS date_message_logged,
    SPLIT(raw_msg, ',') [10] AS time_message_logged,
    SPLIT(raw_msg, ',') [11] AS callsign,
    SPLIT(raw_msg, ',') [12] AS altitude,
    SPLIT(raw_msg, ',') [13] AS ground_speed,
    SPLIT(raw_msg, ',') [14] AS track,
    SPLIT(raw_msg, ',') [15] AS latitude,
    SPLIT(raw_msg, ',') [16] AS longitude,
    SPLIT(raw_msg, ',') [17] AS vertical_rate,
    SPLIT(raw_msg, ',') [18] AS squawk,
    SPLIT(raw_msg, ',') [19] AS alert,
    SPLIT(raw_msg, ',') [20] AS emergency,
    SPLIT(raw_msg, ',') [21] AS spi,
    SPLIT(raw_msg, ',') [22] AS is_on_ground
FROM adsb_raw_stream PARTITION BY SPLIT(raw_msg, ',') [5] EMIT CHANGES;