import gzip
from decimal import Decimal

from jobs.meta_to_silver import _transform, _write_corrupt

VALID = '{"parent_asin": "A1", "title": "Soap", "price": "9.99", "categories": ["Beauty"]}'
TEXT_PRICE = '{"parent_asin": "A2", "price": "$5"}'
NO_KEY = '{"title": "No key"}'
WRONG_TYPE = '{"parent_asin": "A3", "rating_number": "abc"}'
BROKEN_JSON = '{"parent_asin": "A4", "title": '
EMPTY = '{}'
INVALID = [NO_KEY, WRONG_TYPE, BROKEN_JSON, EMPTY]


def test_transform_splits_valid_and_invalid(spark):
    lines = [VALID, VALID, TEXT_PRICE, *INVALID]
    raw = spark.createDataFrame([(line,) for line in lines], 'value string')

    products, corrupt = _transform(raw, 'All_Beauty')

    rows = {row.parent_asin: row for row in products.collect()}
    # Duplicate VALID line kept once
    assert sorted(rows) == ['A1', 'A2']
    assert rows['A1'].price == Decimal('9.99')
    assert rows['A1'].categories == ['Beauty']
    assert rows['A1'].category == 'All_Beauty'
    # Unparseable price -> NULL, product kept
    assert rows['A2'].price is None
    assert '_corrupt_record' not in products.columns

    assert corrupt.columns == ['value']
    # Quarantine keeps the original lines
    assert sorted(row.value for row in corrupt.collect()) == sorted(INVALID)


def _read_quarantine(path):
    return [
        line for file in path.glob('*.txt.gz') for line in gzip.open(file, 'rt').read().splitlines()
    ]


def test_write_corrupt_replaces_only_its_category(spark, tmp_path):
    def write(line, category):
        _write_corrupt(spark.createDataFrame([(line,)], 'value string'), str(tmp_path), category)

    write(BROKEN_JSON, 'Books')
    write(EMPTY, 'All_Beauty')
    write(NO_KEY, 'All_Beauty')

    # Other categories' quarantine survives, this category's is replaced
    assert _read_quarantine(tmp_path / 'category=Books') == [BROKEN_JSON]
    assert _read_quarantine(tmp_path / 'category=All_Beauty') == [NO_KEY]
