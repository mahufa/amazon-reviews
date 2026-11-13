from datetime import datetime

from jobs.silver_to_gold import _transform

SILVER_SCHEMA = """
    rating DOUBLE, title STRING, text STRING, asin STRING, parent_asin STRING,
    user_id STRING, timestamp TIMESTAMP, helpful_vote INT, verified_purchase BOOLEAN,
    year INT, month INT, category STRING
"""


def silver_review(**overrides) -> dict:
    fields = {
        'rating': 4.0,
        'title': 'Nice',
        'text': 'Works well',
        'asin': 'A1',
        'parent_asin': 'P1',
        'user_id': 'U1',
        'timestamp': datetime(2023, 1, 31, 23, 59, 59),
        'helpful_vote': 2,
        'verified_purchase': True,
        'year': 2023,
        'month': 1,
        'category': 'All_Beauty',
    }
    return fields | overrides


def test_transform_builds_fact_row(spark):
    silver = spark.createDataFrame([silver_review()], SILVER_SCHEMA)

    [row] = _transform(silver).collect()

    assert row.asDict() == {
        'date_key': 20230131,
        'review_ts': datetime(2023, 1, 31, 23, 59, 59),
        'parent_asin': 'P1',
        'asin': 'A1',
        'user_id': 'U1',
        'rating': 4.0,
        'helpful_vote': 2,
        'verified_purchase': True,
        'text_length': 10,
        'year': 2023,
        'month': 1,
        'category': 'All_Beauty',
    }


def test_transform_keeps_nulls_in_optional_fields(spark):
    silver = spark.createDataFrame(
        [silver_review(text=None, helpful_vote=None, verified_purchase=None)], SILVER_SCHEMA
    )

    [row] = _transform(silver).collect()

    assert (row.text_length, row.helpful_vote, row.verified_purchase) == (None, None, None)
