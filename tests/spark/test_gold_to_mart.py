import pytest
from jobs.gold_to_mart import _transform

FACT_SCHEMA = """
    parent_asin STRING, rating DOUBLE, helpful_vote INT, verified_purchase BOOLEAN,
    year INT, month INT, category STRING
"""
DIM_SCHEMA = 'parent_asin STRING, store STRING'


def fact(**overrides) -> dict:
    fields = {
        'parent_asin': 'P1',
        'rating': 5.0,
        'helpful_vote': 1,
        'verified_purchase': True,
        'year': 2023,
        'month': 1,
        'category': 'All_Beauty',
    }
    return fields | overrides


def transform(spark, facts: list[dict], dim: list[tuple]) -> dict:
    mart = _transform(
        spark.createDataFrame(facts, FACT_SCHEMA),
        spark.createDataFrame(dim, DIM_SCHEMA),
    )
    return {(row.category, row.store): row for row in mart.collect()}


def test_transform_aggregates_per_store_and_category(spark):
    rows = transform(
        spark,
        [
            fact(parent_asin='P1', rating=5.0, helpful_vote=3, verified_purchase=True),
            fact(parent_asin='P1', rating=2.0, helpful_vote=None, verified_purchase=False),
            fact(parent_asin='P2', rating=5.0, helpful_vote=1, verified_purchase=True),
            fact(parent_asin='P3', rating=1.0),
            # Same store in another category is a separate row
            fact(parent_asin='P4', category='Video_Games'),
        ],
        [('P1', 'Acme'), ('P2', 'Acme'), ('P3', 'Other'), ('P4', 'Acme')],
    )

    assert sorted(rows) == [
        ('All_Beauty', 'Acme'),
        ('All_Beauty', 'Other'),
        ('Video_Games', 'Acme'),
    ]
    acme = rows[('All_Beauty', 'Acme')]
    assert (acme.year, acme.month) == (2023, 1)
    assert acme.review_count == 3
    assert acme.avg_rating == pytest.approx(4.0)
    assert acme.verified_share == pytest.approx(2 / 3)
    assert acme.helpful_votes == 4
    assert acme.product_count == 2


def test_transform_keeps_reviews_without_store_under_null(spark):
    rows = transform(
        spark,
        [
            # Product in dim_product but with no store
            fact(parent_asin='P1'),
            # Product missing from dim_product
            fact(parent_asin='P9'),
        ],
        [('P1', None)],
    )

    assert list(rows) == [('All_Beauty', None)]
    assert rows[('All_Beauty', None)].review_count == 2
    assert rows[('All_Beauty', None)].product_count == 2


def test_transform_null_measures(spark):
    rows = transform(
        spark,
        [fact(helpful_vote=None, verified_purchase=None)],
        [('P1', 'Acme')],
    )

    row = rows[('All_Beauty', 'Acme')]
    # No votes at all -> 0, not NULL; unknown verified flag stays unknown
    assert row.helpful_votes == 0
    assert row.verified_share is None
