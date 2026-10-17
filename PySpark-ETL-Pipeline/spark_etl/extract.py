"""Extract: read the raw HCV CSV into a bronze DataFrame."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

# Source header -> target column name. With an explicit schema Spark maps CSV
# columns by position, so this order must match the file. Everything is read
# as a string so a malformed value never kills the read; typing happens in
# transform, where a failed cast is detected and quarantined, not silently nulled.
SOURCE_COLUMNS = {
    "_c0": "patient_id",
    "Category": "category",
    "Age": "age",
    "Sex": "sex",
    "ALB": "alb",
    "ALP": "alp",
    "ALT": "alt",
    "AST": "ast",
    "BIL": "bil",
    "CHE": "che",
    "CHOL": "chol",
    "CREA": "crea",
    "GGT": "ggt",
    "PROT": "prot",
}

RAW_SCHEMA = StructType([StructField(name, StringType(), True) for name in SOURCE_COLUMNS.values()])


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    df = (
        spark.read.option("header", "true")
        .option("nullValue", "NA")
        .option("mode", "PERMISSIVE")
        # Map by position; the source header has an empty first column name.
        .option("enforceSchema", "true")
        .schema(RAW_SCHEMA)
        .csv(path)
    )
    return df.withColumn("_source_file", F.col("_metadata.file_path")).withColumn(
        "_ingested_at", F.current_timestamp()
    )
