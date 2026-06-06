import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from schema import FIELDS, SCHEMA


def flush(records: list[dict[str, Any]], output_dir: str) -> None:
    if not records:
        return

    by_partition: dict[tuple[int, int, int, int], list[dict[str, Any]]] = defaultdict(
        list
    )
    for rec in records:
        ts: datetime = rec["kafka_timestamp"]
        by_partition[(ts.year, ts.month, ts.day, ts.hour)].append(rec)

    for (year, month, day, hour), group in by_partition.items():
        path = os.path.join(
            output_dir,
            f"year={year}",
            f"month={month:02d}",
            f"day={day:02d}",
            f"hour={hour:02d}",
        )
        os.makedirs(path, exist_ok=True)

        columns: dict[str, list[Any]] = {f: [] for f in FIELDS}
        columns["kafka_timestamp"] = []
        for rec in group:
            for f in FIELDS:
                columns[f].append(rec.get(f))
            columns["kafka_timestamp"].append(rec["kafka_timestamp"])

        table = pa.table(columns, schema=SCHEMA)
        dest = os.path.join(path, f"{uuid4()}.parquet")
        pq.write_table(table, dest)
        sys.stderr.write(f"consumer: wrote {len(group)} records to {dest}\n")
