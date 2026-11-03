"""Expose the pipeline's output as SQL tables over JDBC (Spark Thrift Server).

SQL clients such as DBeaver, Beeline, Power BI or Tableau connect with the
HiveServer2 protocol:

    jdbc:hive2://localhost:10000/hcv

Run after the pipeline has written its output:

    python -m spark_etl.serve --output output/
"""

import argparse
import logging
import signal
import threading
from pathlib import Path

from pyspark.sql import SparkSession

from .config import PipelineConfig

log = logging.getLogger("spark_etl.serve")


def build_server_spark(output_path: str, host: str, port: int, master: str) -> SparkSession:
    # Keep the metastore and warehouse next to the data, not in the CWD, so
    # table definitions persist across restarts and don't litter the repo.
    base = Path(output_path).resolve()
    metastore = base / "_metastore"
    return (
        SparkSession.builder.appName("hcv-sql-server")
        .master(master)
        .enableHiveSupport()
        .config("spark.sql.warehouse.dir", str(base / "_warehouse"))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore / 'metastore_db'};create=true",
        )
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={metastore}")
        .config("spark.hadoop.hive.server2.thrift.bind.host", host)
        .config("spark.hadoop.hive.server2.thrift.port", str(port))
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def register_tables(spark: SparkSession, config: PipelineConfig, database: str) -> list:
    """Create external tables over the Parquet output; data is not copied."""
    tables = {
        "patients": config.silver_path,
        "diagnosis_summary": config.gold_path,
        "quarantine": config.quarantine_path,
    }
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    registered = []
    for name, path in tables.items():
        if not Path(path).exists():
            log.warning("Skipping %s.%s: %s does not exist (run the pipeline first)", database, name, path)
            continue
        # Drop + create picks up schema changes from the latest pipeline run.
        spark.sql(f"DROP TABLE IF EXISTS {database}.{name}")
        spark.sql(f"CREATE TABLE {database}.{name} USING parquet LOCATION '{Path(path).resolve()}'")
        # Silver is partitioned by diagnosis_code; make the metastore aware of them.
        if name == "patients":
            spark.sql(f"MSCK REPAIR TABLE {database}.{name}")
        registered.append(f"{database}.{name}")
    if not registered:
        raise SystemExit(f"No pipeline output found under {config.output_path}")
    return registered


def start_thrift_server(spark: SparkSession) -> None:
    jvm = spark.sparkContext._jvm
    jvm.org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.startWithContext(
        spark._jsparkSession.sqlContext()
    )


def main(argv=None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", required=True, help="Pipeline output directory (the pipeline's --output)")
    p.add_argument("--database", default="hcv")
    p.add_argument(
        "--host", default="localhost", help="Bind address; use 0.0.0.0 to accept remote clients (no auth!)"
    )
    p.add_argument("--port", type=int, default=10000)
    p.add_argument("--master", default="local[*]")
    a = p.parse_args(argv)

    config = PipelineConfig(input_path="", output_path=a.output)
    spark = build_server_spark(a.output, a.host, a.port, a.master)
    tables = register_tables(spark, config, a.database)
    start_thrift_server(spark)
    log.info("Registered tables: %s", ", ".join(tables))
    log.info("Thrift server listening on jdbc:hive2://%s:%d/%s  (Ctrl+C to stop)", a.host, a.port, a.database)

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    stop.wait()
    log.info("Shutting down")
    spark.stop()


if __name__ == "__main__":
    main()
