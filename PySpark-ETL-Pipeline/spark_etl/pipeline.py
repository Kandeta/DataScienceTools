"""End-to-end HCV ETL job: extract -> cast -> validate -> dedupe -> enrich -> load.

Run locally:      python -m spark_etl.pipeline --input data/hcvdata.csv --output out/
Run on a cluster: spark-submit --py-files spark_etl.zip run_pipeline.py --input ... --output ...
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

from . import extract, load, quality, transform
from .config import PipelineConfig, build_spark

log = logging.getLogger("spark_etl")


def run(spark: SparkSession, config: PipelineConfig) -> quality.DQReport:
    bronze = extract.read_raw(spark, config.input_path)
    typed = transform.cast_and_standardise(bronze)
    # typed feeds several actions (counts, quarantine, silver); persist it so the
    # CSV is parsed once rather than once per action.
    typed.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        input_rows = typed.count()
        log.info("Read %d rows from %s", input_rows, config.input_path)

        valid, quarantined = quality.split_valid_invalid(typed)
        valid_rows = valid.count()
        quarantined_rows = input_rows - valid_rows

        silver = transform.enrich(transform.deduplicate(valid)).persist(StorageLevel.MEMORY_AND_DISK)
        silver_rows = silver.count()

        report = quality.DQReport(
            input_rows=input_rows,
            valid_rows=valid_rows,
            quarantined_rows=quarantined_rows,
            duplicates_dropped=valid_rows - silver_rows,
            silver_rows=silver_rows,
            reject_ratio=round(quarantined_rows / input_rows, 4) if input_rows else 1.0,
            failures_by_rule=quality.failures_by_rule(quarantined),
            null_counts=quality.null_counts(silver, transform.LAB_COLUMNS),
            checks=quality.dataset_checks(silver, input_rows, quarantined_rows, config.max_reject_ratio),
        )
        load.write_dq_report(report.to_dict(), config.dq_report_path)

        # Always written (even empty, even on failure) so it reflects this run
        # and bad rows can be inspected.
        load.write_parquet(quarantined, config.quarantine_path)
        if quarantined_rows:
            log.warning("Quarantined %d rows: %s", quarantined_rows, report.failures_by_rule)

        if not report.passed:
            failed = [c["check"] for c in report.checks if not c["passed"]]
            # Do not publish silver/gold when the batch fails its checks.
            raise quality.DataQualityError(f"Dataset checks failed: {failed}; see {config.dq_report_path}")

        load.write_parquet(silver, config.silver_path, partition_by=["diagnosis_code"])
        load.write_parquet(transform.build_gold_summary(silver).coalesce(1), config.gold_path)
        log.info("Wrote %d silver rows to %s", silver_rows, config.silver_path)
        silver.unpersist()
        return report
    finally:
        typed.unpersist()


def parse_args(argv=None) -> PipelineConfig:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", required=True, help="CSV file or directory")
    p.add_argument("--output", required=True, help="Base output directory")
    p.add_argument("--max-reject-ratio", type=float, default=0.05)
    p.add_argument("--shuffle-partitions", type=int, default=8)
    p.add_argument("--master", default=None, help="Spark master; defaults to local[*] outside spark-submit")
    a = p.parse_args(argv)
    return PipelineConfig(
        input_path=a.input,
        output_path=a.output,
        max_reject_ratio=a.max_reject_ratio,
        shuffle_partitions=a.shuffle_partitions,
        master=a.master,
    )


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    config = parse_args(argv)
    spark = build_spark(config)
    try:
        report = run(spark, config)
        log.info("Pipeline succeeded: %d in, %d published", report.input_rows, report.silver_rows)
        return 0
    except quality.DataQualityError as exc:
        log.error("%s", exc)
        return 2
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
