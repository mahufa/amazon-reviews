import gzip
from decimal import Decimal

from jobs.meta_to_silver import _extract_cat, _transform, _write_corrupt

VALID = '{"parent_asin": "A1", "title": "Soap", "price": "9.99", "categories": ["Beauty"]}'
TEXT_PRICE = '{"parent_asin": "A2", "price": "$5"}'
NO_KEY = '{"title": "No key"}'
WRONG_TYPE = '{"parent_asin": "A3", "rating_number": "abc"}'
BROKEN_JSON = '{"parent_asin": "A4", "title": '
EMPTY = '{}'
INVALID = [NO_KEY, WRONG_TYPE, BROKEN_JSON, EMPTY]


def test_transform_splits_valid_and_invalid(spark):
    lines = [VALID, VALID, TEXT_PRICE, *INVALID]
    raw = spark.createDataFrame(
        [(line, 'All_Beauty') for line in lines],
        'value string, category string',
    )

    products, corrupt = _transform(raw)

    rows = {row.parent_asin: row for row in products.collect()}
    # Duplicate VALID line kept once
    assert sorted(rows) == ['A1', 'A2']
    assert rows['A1'].price == Decimal('9.99')
    assert rows['A1'].categories == ['Beauty']
    assert rows['A1'].category == 'All_Beauty'
    # Unparseable price -> NULL, product kept
    assert rows['A2'].price is None
    assert '_corrupt_record' not in products.columns

    assert corrupt.columns == ['value', 'category']
    # Quarantine keeps the original lines
    assert sorted(row.value for row in corrupt.collect()) == sorted(INVALID)


def test_extract_cat_reads_category_from_file_name(spark, tmp_path):
    source = tmp_path / 'meta_All_Beauty.jsonl'
    source.write_text(VALID + '\n')

    rows = spark.read.text(str(source)).select(_extract_cat().alias('category')).collect()

    assert [row.category for row in rows] == ['All_Beauty']


def test_write_corrupt_replaces_whole_quarantine(spark, tmp_path):
    schema = 'value string, category string'
    _write_corrupt(spark.createDataFrame([(BROKEN_JSON, 'Books')], schema), str(tmp_path))
    _write_corrupt(spark.createDataFrame([(NO_KEY, 'All_Beauty')], schema), str(tmp_path))

    # Static overwrite: Books from the earlier run is gone
    assert sorted(p.name for p in tmp_path.glob('category=*')) == ['category=All_Beauty']
    lines = [
        line
        for path in tmp_path.glob('category=All_Beauty/*.txt.gz')
        for line in gzip.open(path, 'rt').read().splitlines()
    ]
    assert lines == [NO_KEY]
