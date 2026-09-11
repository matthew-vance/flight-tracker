# Flight Tracker

ADS-B flight tracking pipeline: a dump1090 receiver feeds Redpanda, ksqlDB
derives per-aircraft state, an Express API streams it over SSE, and a Parquet
sink keeps raw messages for offline analysis.

## Architecture

```
dump1090 (port 30003, SBS/BaseStation CSV)
   │
   ▼
producer/  (Python, runs on the host)
   ingest  → tails the TCP socket, buffers each line into SQLite (data/buffer.db)
   publish → drains SQLite in batches → topic adsb.raw (key = ICAO hex)
   │
   ▼
Redpanda   adsb.raw → adsb.parsed → adsb.state (compacted)
   │
   ├─ ksqldb-server   runs ksqldb/processor/queries.sql on boot
   │     adsb.raw    → SPLIT on commas            → adsb.parsed (JSON_SR)
   │     adsb.parsed → LATEST_BY_OFFSET per icao  → adsb.state
   │
   ├─ api/  (Node 24 + Express, :3000)
   │     consumes adsb.state → in-memory map (60s TTL)
   │     GET /events → SSE: aircraft.updated / aircraft.removed
   │                   (replays the current snapshot on connect)
   │
   └─ consumer/  (Python)
         consumes adsb.raw → Parquet every 5 min
         → consumer/data/year=/month=/day=/hour=/  (Hive-partitioned)

notebooks/  Jupyter + DuckDB + pandas over the Parquet files
```

## Prerequisites

- Docker
- [just](https://github.com/casey/just), [uv](https://docs.astral.sh/uv/), Node 24 + pnpm (only for running services on the host)
- A reachable dump1090 instance exposing port 30003

## Running

```sh
# 1. Everything except the producer
docker compose up -d --build

# 2. Producer, on the host so it can reach dump1090
DUMP1090_HOST=<receiver-ip> just producer

# 3. Check data is flowing
curl -N localhost:3000/events
```

Compose brings up:

| Service         | URL / port              | Notes                                   |
| --------------- | ----------------------- | --------------------------------------- |
| redpanda-0      | `localhost:19092`       | Kafka external listener                 |
| init-redpanda   | —                       | creates the three topics, then exits    |
| console         | http://localhost:8080   | Redpanda Console — browse topics here   |
| ksqldb-server   | —                       | auto-runs `ksqldb/processor/queries.sql` |
| api             | http://localhost:3000   | `GET /events` SSE stream                |
| consumer        | —                       | writes Parquet to `./consumer/data`     |

### Running services on the host instead

Each service defaults to `localhost:19092`, so they work against the compose
Redpanda without extra config:

```sh
just api
just consumer
just notebooks     # Jupyter Lab
```

### Configuration

| Variable                | Default           | Used by  |
| ----------------------- | ----------------- | -------- |
| `DUMP1090_HOST`         | `localhost`       | producer |
| `DUMP1090_PORT`         | `30003`           | producer |
| `BUFFER_DB_PATH`        | `data/buffer.db`  | producer |
| `REDPANDA_BROKER`       | `localhost:19092` | producer, consumer |
| `KAFKA_BROKERS`         | `localhost:19092` | api      |
| `PORT`                  | `3000`            | api      |
| `OUTPUT_DIR`            | `data`            | consumer |
| `FLUSH_INTERVAL_SECONDS`| `300`             | consumer |

## Notes

- The producer buffers to SQLite so no messages are lost while Redpanda is
  down; anything left in `producer/data/buffer.db` is published on next start.
- The producer retries the dump1090 connection every 5s and logs
  `connection failed, retrying...` until the receiver is reachable.
- Redpanda data lives in a named volume and survives `docker compose down`.
  Use `down -v` to start from scratch.
