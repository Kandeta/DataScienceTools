from spark_etl import extract, quality, transform

GOOD = '"1","0=Blood Donor",32,"m",38.5,52.5,7.7,22.1,7.5,6.93,3.23,106,12.1,69'


def silver_from(spark, path):
    typed = transform.cast_and_standardise(extract.read_raw(spark, path))
    valid, invalid = quality.split_valid_invalid(typed)
    return transform.enrich(transform.deduplicate(valid)), invalid


def test_cast_and_standardise_parses_category_and_types(spark, write_csv):
    path = write_csv(GOOD, '"2","0s=suspect Blood Donor",47,"f",NA,NA,10,25,5,7,NA,80,20,70')
    rows = {r.patient_id: r for r in transform.cast_and_standardise(extract.read_raw(spark, path)).collect()}

    assert rows["1"].diagnosis_code == "0" and rows["1"].diagnosis == "Blood Donor"
    assert rows["2"].diagnosis_code == "0s" and rows["2"].diagnosis == "suspect Blood Donor"
    assert rows["1"].sex == "M" and rows["2"].sex == "F"
    assert rows["1"].age == 32 and rows["1"].alb == 38.5
    # "NA" is a legitimate missing value, not a type error.
    assert rows["2"].alb is None and rows["2"]._type_errors == []


def test_unparseable_number_is_flagged_not_silently_nulled(spark, write_csv):
    path = write_csv('"1","1=Hepatitis",abc,"m",38.5,52.5,7.7,22.1,7.5,6.93,3.23,106,oops,69')
    row = transform.cast_and_standardise(extract.read_raw(spark, path)).first()
    assert row.age is None
    assert sorted(row._type_errors) == ["age", "ggt"]


def test_invalid_rows_are_quarantined_with_reasons(spark, write_csv):
    path = write_csv(
        GOOD,
        '"2","9=Unknown",40,"m",38.5,52.5,7.7,22.1,7.5,6.93,3.23,106,12.1,69',  # bad code
        '"3","1=Hepatitis",150,"x",38.5,52.5,7.7,22.1,7.5,6.93,3.23,106,12.1,69',  # bad age + sex
        '"4","2=Fibrosis",50,"f",38.5,-1,7.7,22.1,7.5,6.93,3.23,106,12.1,69',  # negative ALP
        '"","3=Cirrhosis",50,"f",38.5,52.5,7.7,22.1,7.5,6.93,3.23,106,12.1,69',  # missing id
    )
    silver, invalid = silver_from(spark, path)
    reasons = {r.patient_id: sorted(r._failed_rules) for r in invalid.collect()}

    assert [r.patient_id for r in silver.collect()] == ["1"]
    assert reasons["2"] == ["diagnosis_code_valid"]
    assert reasons["3"] == ["age_in_range", "sex_valid"]
    assert reasons["4"] == ["alp_non_negative"]
    assert reasons[None] == ["patient_id_not_null"]


def test_deduplicate_keeps_most_complete_record(spark, write_csv):
    path = write_csv(
        '"1","0=Blood Donor",32,"m",NA,NA,7.7,22.1,7.5,6.93,3.23,106,12.1,69',
        GOOD,
    )
    silver, _ = silver_from(spark, path)
    rows = silver.collect()
    assert len(rows) == 1
    assert rows[0].alb == 38.5 and rows[0].lab_values_missing == 0


def test_enrich_derives_features(spark, write_csv):
    path = write_csv(
        '"1","3=Cirrhosis",65,"m",38.5,52.5,10,30,7.5,6.93,3.23,106,12.1,69',
        '"2","0=Blood Donor",25,"f",38.5,52.5,0,30,7.5,6.93,3.23,106,12.1,69',
    )
    silver, _ = silver_from(spark, path)
    rows = {r.patient_id: r for r in silver.collect()}

    assert rows["1"].age_band == "60+" and rows["2"].age_band == "<30"
    assert rows["1"].de_ritis_ratio == 3.0
    assert rows["2"].de_ritis_ratio is None  # ALT of 0 must not divide
    assert rows["1"].is_hcv_positive is True and rows["2"].is_hcv_positive is False
