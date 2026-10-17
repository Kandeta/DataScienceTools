"""Load: write silver, quarantine and gold layers plus the DQ report."""

import json
from pathlib import Path

from pyspark.sql import DataFrame


def write_parquet(df: DataFrame, path: str, partition_by=None) -> None:
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.parquet(path)


def write_dq_report(report: dict, path: str) -> None:
    # Driver-side write; fine for a local or mounted path. On object storage,
    # swap this for the cloud SDK or write it as a one-row JSON DataFrame.
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(report, indent=2, default=str))
