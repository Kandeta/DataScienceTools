# PySpark ETL Pipeline

A small but production-shaped PySpark batch job. It ingests the HCV liver-panel
dataset (`data/hcvdata.csv`, 615 patients), validates it, and publishes
partitioned Parquet layers plus a data-quality report.

```
CSV (bronze) ──► cast & standardise ──► row rules ──┬──► quarantine/   (rows + failed rule names)
                                                    └──► dedupe ──► enrich ──► silver/  (partitioned by diagnosis_code)
                                                                              └──► gold/    (per-diagnosis summary)
                                          dataset checks ──► dq_report.json  (fails the run if breached)
```

## What it does

| Stage | Module | Details |
|---|---|---|
| Extract | `spark_etl/extract.py` | Explicit all-string schema, `NA` → null, adds `_source_file` and `_ingested_at` lineage columns. |
| Transform | `spark_etl/transform.py` | `try_cast` to typed columns while recording which fields failed to parse; splits `"0s=suspect Blood Donor"` into `diagnosis_code` / `diagnosis`; normalises sex. |
| Validate | `spark_etl/quality.py` | Row rules (valid id, diagnosis code, sex, age 0–120, non-negative labs, no type errors). Failing rows go to quarantine with an array of the rules they broke. |
| Dedupe | `transform.deduplicate` | One row per `patient_id`, keeping the most complete lab record (window + `row_number`). |
| Enrich | `transform.enrich` | `age_band`, De Ritis ratio (AST/ALT, guarded against ALT = 0), `is_hcv_positive`, `lab_values_missing`. |
| Load | `spark_etl/load.py` | Snappy Parquet; silver partitioned by `diagnosis_code` with dynamic partition overwrite, so reruns are idempotent. |
| Gate | `quality.dataset_checks` | Non-empty input/output, unique `patient_id`, reject ratio ≤ `--max-reject-ratio`. On failure silver/gold are **not** written (the last good version stays) and the job exits with code 2. |

## Run it

```bash
pip install -r requirements.txt     # needs Java 17+

# local
python -m spark_etl.pipeline --input data/hcvdata.csv --output output/

# cluster (zip the package so executors can import it)
zip -r spark_etl.zip spark_etl
spark-submit --master yarn --py-files spark_etl.zip run_pipeline.py \
  --input s3a://bucket/raw/hcv/ --output s3a://bucket/curated/hcv/ --max-reject-ratio 0.02
```

Output layout:

```
output/
├── silver/patients/diagnosis_code=0/…parquet
├── quarantine/patients/…parquet
├── gold/diagnosis_summary/…parquet
└── dq_report.json
```

Sample gold output from the bundled data:

| diagnosis_code | diagnosis | patients | avg_age | median_alt | median_ast | avg_de_ritis_ratio |
|---|---|---|---|---|---|---|
| 0 | Blood Donor | 533 | 47.1 | 23.1 | 24.8 | 1.154 |
| 0s | suspect Blood Donor | 7 | 57.6 | 49.2 | 46.7 | 1.427 |
| 1 | Hepatitis | 24 | 38.7 | 15.2 | 46.0 | 3.791 |
| 2 | Fibrosis | 21 | 52.3 | 34.0 | 70.0 | 4.729 |
| 3 | Cirrhosis | 30 | 53.5 | 5.4 | 90.4 | 16.844 |

## Tests

```bash
python -m pytest -q tests
```

The suite covers parsing, type-error detection, every quarantine rule,
deduplication, derived features, an end-to-end run on the sample data, and the
"too many rejects → fail and don't publish" path.

## Design notes

- **Read as strings, cast with `try_cast`.** A typed CSV schema silently turns
  `"abc"` into null; reading strings first lets the job tell a genuine missing
  value (`NA`) apart from a broken one and quarantine the latter.
- **Persist once, act many.** The typed frame feeds several counts and writes,
  so it is persisted with `MEMORY_AND_DISK` and released in a `finally`.
- **Validate before dedupe.** Null ids are quarantined first, so the dedupe
  window never lumps unrelated null-id rows into one partition.
- **The DQ report is always written**, pass or fail, so an orchestrator (e.g.
  an Airflow task) can read it and alert on `passed: false`.
