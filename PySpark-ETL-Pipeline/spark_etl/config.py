"""Pipeline configuration and SparkSession factory."""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession


@dataclass(frozen=True)
class PipelineConfig:
    input_path: str
    output_path: str
    # Fail the run if more than this share of rows is quarantined.
    max_reject_ratio: float = 0.05
    shuffle_partitions: int = 8
    app_name: str = "hcv-etl"
    master: Optional[str] = None
    extra_spark_conf: dict = field(default_factory=dict)

    @property
    def silver_path(self) -> str:
        return str(Path(self.output_path) / "silver" / "patients")

    @property
    def quarantine_path(self) -> str:
        return str(Path(self.output_path) / "quarantine" / "patients")

    @property
    def gold_path(self) -> str:
        return str(Path(self.output_path) / "gold" / "diagnosis_summary")

    @property
    def dq_report_path(self) -> str:
        return str(Path(self.output_path) / "dq_report.json")


def build_spark(config: PipelineConfig) -> SparkSession:
    """Create (or reuse) a SparkSession tuned for this job.

    Under spark-submit the master comes from the submit command; run as a
    plain Python script it falls back to local mode.
    """
    builder = (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
        .config("spark.sql.adaptive.enabled", "true")
        # Only overwrite the partitions we actually write, not the whole table.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if config.master:
        builder = builder.master(config.master)
    elif not SparkConf().contains("spark.master"):
        builder = builder.master("local[*]")
    for key, value in config.extra_spark_conf.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
