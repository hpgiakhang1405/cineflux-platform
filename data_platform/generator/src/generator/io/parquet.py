"""Serialize validated batch records as compressed Parquet files.

Reads: in-memory record dictionaries produced by batch generators.
Writes: Parquet bytes ready for an ObjectWriter gateway.
Runs: bootstrap and recurring batch runners.
"""

from io import BytesIO
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


def serialize_parquet(records: list[dict[str, Any]], compression: str) -> bytes:
    """Serialize one non-empty record batch to Parquet bytes.

    Args:
        records: Validated records with a consistent physical schema.
        compression: PyArrow Parquet compression codec.

    Returns:
        Complete Parquet file bytes.

    Raises:
        ValueError: When the record list is empty.
    """
    if not records:
        raise ValueError("Cannot serialize an empty Parquet file")
    buffer = BytesIO()
    table = pa.Table.from_pylist(records)
    pq.write_table(table, buffer, compression=compression)
    return buffer.getvalue()
