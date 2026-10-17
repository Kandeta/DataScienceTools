"""Data quality: row-level rules (quarantine) and dataset-level checks (fail the run)."""

from dataclasses import asdict, dataclass
from typing import Dict, List, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from .transform import LAB_COLUMNS

VALID_DIAGNOSIS_CODES = ["0", "0s", "1", "2", "3"]
VALID_SEX = ["M", "F"]


class DataQualityError(Exception):
    pass


def row_rules() -> Dict[str, Column]:
    """Rule name -> condition that must be TRUE for a row to be kept.

    Lab values are allowed to be missing (the source uses NA), but when
    present they must be non-negative.
    """
    rules = {
        "patient_id_not_null": F.col("patient_id").isNotNull(),
        "diagnosis_code_valid": F.col("diagnosis_code").isin(VALID_DIAGNOSIS_CODES),
        "sex_valid": F.col("sex").isin(VALID_SEX),
        "age_in_range": F.col("age").between(0, 120),
        "no_type_errors": F.size("_type_errors") == 0,
    }
    for c in LAB_COLUMNS:
        rules[f"{c}_non_negative"] = F.col(c).isNull() | (F.col(c) >= 0)
    return rules


def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Tag each row with the rules it fails; return (valid, quarantined)."""
    # coalesce: a rule evaluating to NULL (e.g. isin on a null) counts as a failure.
    failed = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in row_rules().items()]
    tagged = df.withColumn("_failed_rules", F.filter(F.array(*failed), lambda x: x.isNotNull()))
    valid = tagged.filter(F.size("_failed_rules") == 0).drop("_failed_rules", "_type_errors")
    invalid = tagged.filter(F.size("_failed_rules") > 0).withColumn("_quarantined_at", F.current_timestamp())
    return valid, invalid


@dataclass
class DQReport:
    input_rows: int
    valid_rows: int
    quarantined_rows: int
    duplicates_dropped: int
    silver_rows: int
    reject_ratio: float
    failures_by_rule: Dict[str, int]
    null_counts: Dict[str, int]
    checks: List[Dict]

    @property
    def passed(self) -> bool:
        return all(c["passed"] for c in self.checks)

    def to_dict(self) -> dict:
        return {**asdict(self), "passed": self.passed}


def failures_by_rule(quarantined: DataFrame) -> Dict[str, int]:
    rows = quarantined.select(F.explode("_failed_rules").alias("rule")).groupBy("rule").count().collect()
    return {r["rule"]: r["count"] for r in sorted(rows, key=lambda r: r["rule"])}


def null_counts(df: DataFrame, columns: List[str]) -> Dict[str, int]:
    row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]).first()
    return {c: int(row[c] or 0) for c in columns}


def dataset_checks(silver: DataFrame, input_rows: int, quarantined_rows: int, max_reject_ratio: float) -> List[Dict]:
    silver_rows = silver.count()
    distinct_ids = silver.select("patient_id").distinct().count()
    reject_ratio = quarantined_rows / input_rows if input_rows else 1.0
    return [
        {"check": "input_not_empty", "passed": input_rows > 0, "value": input_rows},
        {"check": "silver_not_empty", "passed": silver_rows > 0, "value": silver_rows},
        {"check": "patient_id_unique", "passed": distinct_ids == silver_rows, "value": silver_rows - distinct_ids},
        {
            "check": "reject_ratio_within_threshold",
            "passed": reject_ratio <= max_reject_ratio,
            "value": round(reject_ratio, 4),
            "threshold": max_reject_ratio,
        },
    ]
