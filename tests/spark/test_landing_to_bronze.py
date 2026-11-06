from jobs.landing_to_bronze import _transform

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
