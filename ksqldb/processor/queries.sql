CREATE STREAM IF NOT EXISTS adsb_raw_stream (raw_msg VARCHAR) WITH (
    KAFKA_TOPIC = 'adsb.raw',
    VALUE_FORMAT = 'KAFKA'
);

CREATE OR REPLACE STREAM adsb_parsed WITH (
    KAFKA_TOPIC = 'adsb.parsed',
    VALUE_FORMAT = 'JSON_SR',
    PARTITIONS = 1
) AS
SELECT
    NULLIF(SPLIT(raw_msg, ',') [5], '') AS hex_ident,
    NULLIF(SPLIT(raw_msg, ',') [11], '') AS callsign,
    NULLIF(SPLIT(raw_msg, ',') [12], '') AS altitude,
    NULLIF(SPLIT(raw_msg, ',') [13], '') AS ground_speed,
    NULLIF(SPLIT(raw_msg, ',') [14], '') AS track,
    NULLIF(SPLIT(raw_msg, ',') [15], '') AS latitude,
    NULLIF(SPLIT(raw_msg, ',') [16], '') AS longitude,
    NULLIF(SPLIT(raw_msg, ',') [18], '') AS squawk
FROM adsb_raw_stream
PARTITION BY NULLIF(SPLIT(raw_msg, ',') [5], '')
EMIT CHANGES;

CREATE TABLE IF NOT EXISTS adsb_state WITH (
    KAFKA_TOPIC = 'adsb.state',
    VALUE_FORMAT = 'JSON_SR',
    PARTITIONS = 1
) AS
SELECT
    hex_ident,
    LATEST_BY_OFFSET(hex_ident) AS icao,
    LATEST_BY_OFFSET(callsign, true) AS flight,
    LATEST_BY_OFFSET(altitude, true) AS altitude,
    LATEST_BY_OFFSET(ground_speed, true) AS speed,
    LATEST_BY_OFFSET(track, true) AS heading,
    LATEST_BY_OFFSET(latitude, true) AS latitude,
    LATEST_BY_OFFSET(longitude, true) AS longitude,
    LATEST_BY_OFFSET(squawk, true) AS squawk
FROM adsb_parsed
WHERE hex_ident IS NOT NULL
GROUP BY hex_ident
EMIT CHANGES;
