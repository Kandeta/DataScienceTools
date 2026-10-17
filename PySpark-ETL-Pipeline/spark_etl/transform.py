"""Transform: bronze (raw strings) -> silver (typed, standardised, enriched)."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

LAB_COLUMNS = ["alb", "alp", "alt", "ast", "bil", "che", "chol", "crea", "ggt", "prot"]

# Columns whose string value must survive a cast; a non-null raw value that
# casts to null is recorded as a type error and the row is quarantined.
TYPED_COLUMNS = {"age": "INT", **{c: "DOUBLE" for c in LAB_COLUMNS}}

HCV_POSITIVE_CODES = ["1", "2", "3"]


def _try_cast(col_name: str, sql_type: str):
    # try_cast returns NULL instead of raising under ANSI mode (default in Spark 4).
    return F.expr(f"try_cast(trim({col_name}) AS {sql_type})")


def cast_and_standardise(df: DataFrame) -> DataFrame:
    type_errors = [
        F.when(F.col(c).isNotNull() & _try_cast(c, t).isNull(), F.lit(c)) for c, t in TYPED_COLUMNS.items()
    ]
    category = F.trim(F.col("category"))

    out = df.select(
        F.when(F.trim("patient_id") != "", F.trim("patient_id")).alias("patient_id"),
        # "0s=suspect Blood Donor" -> code "0s", label "suspect Blood Donor"
        F.regexp_extract(category, r"^([^=]+)=", 1).alias("diagnosis_code"),
        F.regexp_extract(category, r"=(.+)$", 1).alias("diagnosis"),
        F.upper(F.trim("sex")).alias("sex"),
        *[_try_cast(c, t).alias(c) for c, t in TYPED_COLUMNS.items()],
        F.filter(F.array(*type_errors), lambda x: x.isNotNull()).alias("_type_errors"),
        "_source_file",
        "_ingested_at",
    )
    # regexp_extract yields "" on no match; normalise to null so validation sees it.
    for c in ("diagnosis_code", "diagnosis"):
        out = out.withColumn(c, F.when(F.col(c) != "", F.col(c)))
    return out


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per patient_id: the one with the most populated lab values."""
    completeness = sum(F.col(c).isNotNull().cast("int") for c in LAB_COLUMNS)
    w = Window.partitionBy("patient_id").orderBy(completeness.desc(), F.col("_source_file"))
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def enrich(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "age_band",
            F.when(F.col("age") < 30, "<30")
            .when(F.col("age") < 40, "30-39")
            .when(F.col("age") < 50, "40-49")
            .when(F.col("age") < 60, "50-59")
            .when(F.col("age").isNotNull(), "60+"),
        )
        # De Ritis ratio (AST/ALT); > 2 is a classic marker of advanced liver disease.
        .withColumn(
            "de_ritis_ratio",
            F.when(F.col("alt") > 0, F.round(F.col("ast") / F.col("alt"), 3)),
        )
        .withColumn("is_hcv_positive", F.col("diagnosis_code").isin(HCV_POSITIVE_CODES))
        .withColumn("lab_values_missing", sum(F.col(c).isNull().cast("int") for c in LAB_COLUMNS))
    )


def build_gold_summary(silver: DataFrame) -> DataFrame:
    """Per-diagnosis cohort profile for reporting."""
    return (
        silver.groupBy("diagnosis_code", "diagnosis")
        .agg(
            F.count("*").alias("patients"),
            F.round(F.avg("age"), 1).alias("avg_age"),
            F.round(F.avg(F.when(F.col("sex") == "F", 1).otherwise(0)), 3).alias("female_share"),
            F.round(F.percentile_approx("alt", 0.5), 2).alias("median_alt"),
            F.round(F.percentile_approx("ast", 0.5), 2).alias("median_ast"),
            F.round(F.percentile_approx("ggt", 0.5), 2).alias("median_ggt"),
            F.round(F.avg("de_ritis_ratio"), 3).alias("avg_de_ritis_ratio"),
            F.sum("lab_values_missing").alias("missing_lab_values"),
        )
        .orderBy("diagnosis_code")
    )
