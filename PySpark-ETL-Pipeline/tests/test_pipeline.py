import json
from pathlib import Path

import pytest

from spark_etl import pipeline, quality
from spark_etl.config import PipelineConfig

SAMPLE = str(Path(__file__).resolve().parents[1] / "data" / "hcvdata.csv")


def test_sample_dataset_end_to_end(spark, tmp_path):
    config = PipelineConfig(input_path=SAMPLE, output_path=str(tmp_path))
    report = pipeline.run(spark, config)

    assert report.passed
    assert report.input_rows == 615 and report.silver_rows == 615

    silver = spark.read.parquet(config.silver_path)
    assert silver.count() == 615
    assert {r.diagnosis_code for r in silver.select("diagnosis_code").distinct().collect()} == {
        "0", "0s", "1", "2", "3"
    }
    gold = {r.diagnosis: r.patients for r in spark.read.parquet(config.gold_path).collect()}
    assert gold == {
        "Blood Donor": 533,
        "suspect Blood Donor": 7,
        "Hepatitis": 24,
        "Fibrosis": 21,
        "Cirrhosis": 30,
    }
    assert json.loads(Path(config.dq_report_path).read_text())["passed"] is True


def test_batch_over_reject_threshold_fails_and_does_not_publish(spark, tmp_path, write_csv):
    path = write_csv(
        '"1","0=Blood Donor",32,"m",38.5,52.5,7.7,22.1,7.5,6.93,3.23,106,12.1,69',
        '"2","bogus",32,"m",38.5,52.5,7.7,22.1,7.5,6.93,3.23,106,12.1,69',
    )
    config = PipelineConfig(input_path=path, output_path=str(tmp_path / "out"), max_reject_ratio=0.1)

    with pytest.raises(quality.DataQualityError, match="reject_ratio_within_threshold"):
        pipeline.run(spark, config)

    assert not Path(config.silver_path).exists()
    assert spark.read.parquet(config.quarantine_path).count() == 1
    report = json.loads(Path(config.dq_report_path).read_text())
    assert report["passed"] is False and report["failures_by_rule"] == {"diagnosis_code_valid": 1}
