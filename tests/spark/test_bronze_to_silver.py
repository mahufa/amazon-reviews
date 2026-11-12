import json

import pyspark.sql.functions as F
import pytest
from jobs.bronze_to_silver import _read_month, _transform

TS = 1588687728923
PARTITION = (2020, 5, 'All_Beauty')


def review(**overrides) -> str:
    fields = {'rating': 5.0, 'asin': 'A1', 'parent_asin': 'P1', 'user_id': 'U1', 'timestamp': TS}
    return json.dumps(fields | overrides)


def to_partition(spark, lines, category='All_Beauty'):
    year, month, _ = PARTITION
    return spark.createDataFrame(
        [(line, year, month, category) for line in lines],
        'value string, year int, month int, category string',
    )


@pytest.mark.parametrize(
    ('line', 'reason'),
    [
        ('{"user_id": "U1", ', 'corrupt'),
        # Type mismatch also sets _corrupt_record; corrupt wins over the other rules
        ('{"rating": "five"}', 'corrupt'),
        # Valid JSON but not an object
        ('null', 'corrupt'),
        ('[]', 'corrupt'),
        ('123', 'corrupt'),
        (review(user_id=None), 'missing_key'),
        (review(asin=None), 'missing_key'),
        (review(parent_asin=None), 'missing_key'),
        # missing_key wins over invalid_rating
        (review(user_id=None, rating=9.0), 'missing_key'),
        (review(rating=0.0), 'invalid_rating'),
        (review(rating=5.5), 'invalid_rating'),
        (review(rating=None), 'invalid_rating'),
    ],
)
def test_transform_rejects_with_reason(spark, line, reason):
    reviews, rejects = _transform(to_partition(spark, [line]))

    assert reviews.count() == 0
    assert [(row.value, row.reason) for row in rejects.collect()] == [(line, reason)]


def test_transform_keeps_valid_reviews_and_dedups(spark):
    other_user = review(user_id='U2', rating=1.0)
    raw = to_partition(spark, [review(), review(), other_user]).unionByName(
        # Same review in another category file stays: category partitions are independent
        to_partition(spark, [review()], category='Books')
    )

    reviews, rejects = _transform(raw)

    rows = sorted((r.user_id, r.category, r.rating) for r in reviews.collect())
    assert rows == [('U1', 'All_Beauty', 5.0), ('U1', 'Books', 5.0), ('U2', 'All_Beauty', 1.0)]
    assert rejects.count() == 0
    assert '_corrupt_record' not in reviews.columns
    assert {'year', 'month', 'category'} <= set(reviews.columns)
    assert rejects.columns == ['value', 'reason', 'year', 'month', 'category']


def test_transform_dedup_keeps_most_voted_version(spark):
    versions = [review(helpful_vote=v, title=f'v{v}') for v in (0, 3, 1)]
    no_votes = review(user_id='U2', title='kept')

    reviews, _ = _transform(to_partition(spark, [*versions, no_votes]))

    rows = sorted((r.user_id, r.helpful_vote, r.title) for r in reviews.collect())
    # Missing helpful_vote must not null out the other columns
    assert rows == [('U1', 3, 'v3'), ('U2', None, 'kept')]


@pytest.mark.parametrize('ts', [TS, str(TS), float(TS)])
def test_transform_accepts_timestamps_bronze_accepts(spark, ts):
    reviews, rejects = _transform(to_partition(spark, [review(timestamp=ts)]))

    assert rejects.count() == 0
    # unix_millis avoids Python's local-timezone conversion on collect
    assert reviews.select(F.unix_millis('timestamp')).first()[0] == TS


def test_read_month_reads_only_that_month_with_partition_columns(spark, tmp_path):
    for month, category in [(5, 'All_Beauty'), (5, 'Books'), (6, 'All_Beauty')]:
        part = tmp_path / 'year=2020' / f'month={month}' / f'category={category}'
        part.mkdir(parents=True)
        (part / 'part-0.txt').write_text(f'{month}-{category}\n')

    rows = _read_month(spark, str(tmp_path), 2020, 5).collect()

    assert sorted((r.value, r.year, r.month, r.category) for r in rows) == [
        ('5-All_Beauty', 2020, 5, 'All_Beauty'),
        ('5-Books', 2020, 5, 'Books'),
    ]
