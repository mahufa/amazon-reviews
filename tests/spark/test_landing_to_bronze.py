from jobs.landing_to_bronze import _transform, _write_valid

VALID = '{"rating": 5.0, "timestamp": 1588687728923}'
LAST_MS_OF_2022 = '{"rating": 4.0, "timestamp": 1672531199999}'
NO_TIMESTAMP = '{"rating": 3.0}'
BROKEN_JSON = '{"rating": 2.0, "timestamp": 15886'


def test_transform_splits_valid_and_invalid(spark):
    raw = spark.createDataFrame(
        [(line,) for line in (VALID, LAST_MS_OF_2022, NO_TIMESTAMP, BROKEN_JSON)],
        'value string',
    )

    valid, invalid = _transform(raw, 'All_Beauty')

    assert sorted(tuple(row) for row in valid.collect()) == sorted(
        [
            (VALID, 2020, 5, 'All_Beauty'),
            (LAST_MS_OF_2022, 2022, 12, 'All_Beauty'),
        ]
    )
    assert invalid.columns == ['value']
    assert sorted(row.value for row in invalid.collect()) == sorted([NO_TIMESTAMP, BROKEN_JSON])


def test_write_valid_creates_one_file_per_partition(spark, tmp_path):
    rows = [(VALID, 2020, 5, 'All_Beauty'), (LAST_MS_OF_2022, 2022, 12, 'All_Beauty')] * 50
    # Spread every month over several input partitions, like splits of a large file
    valid = spark.createDataFrame(rows, 'value string, year int, month int, category string')
    valid = valid.repartition(4)

    _write_valid(valid, str(tmp_path))

    files = sorted(p.relative_to(tmp_path).parent for p in tmp_path.rglob('*.txt.gz'))
    assert [str(p) for p in files] == [
        'year=2020/month=5/category=All_Beauty',
        'year=2022/month=12/category=All_Beauty',
    ]
