from decimal import Decimal

from jobs.products_to_gold import _transform

SILVER_SCHEMA = """
    parent_asin STRING, title STRING, average_rating DOUBLE, rating_number INT,
    price DECIMAL(10,2), main_category STRING, store STRING, categories ARRAY<STRING>,
    details MAP<STRING, STRING>, category STRING
"""


def silver_product(**overrides) -> dict:
    fields = {
        'parent_asin': 'P1',
        'title': 'Soap',
        'average_rating': 4.5,
        'rating_number': 10,
        'price': Decimal('9.99'),
        'main_category': 'All Beauty',
        'store': 'Acme',
        'categories': ['Beauty', 'Skin Care'],
        'details': {'Brand': 'Acme'},
        'category': 'All_Beauty',
    }
    return fields | overrides


def transform(spark, products: list[dict]) -> dict:
    silver = spark.createDataFrame(products, SILVER_SCHEMA)
    return {row.parent_asin: row for row in _transform(silver).collect()}


def test_transform_builds_dim_row(spark):
    rows = transform(spark, [silver_product()])

    assert rows['P1'].asDict() == {
        'parent_asin': 'P1',
        'title': 'Soap',
        'store': 'Acme',
        'main_category': 'All Beauty',
        'category': 'All_Beauty',
        'category_path': 'Beauty > Skin Care',
        'price': Decimal('9.99'),
        'average_rating': 4.5,
        'rating_number': 10,
    }


def test_transform_keeps_one_row_per_parent_asin(spark):
    rows = transform(
        spark,
        [
            silver_product(category='All_Beauty', rating_number=10),
            silver_product(category='Books', rating_number=50, title='Soap (book)'),
            # NULL rating_number loses to any number
            silver_product(category='Toys', rating_number=None),
            silver_product(parent_asin='P2'),
        ],
    )

    assert sorted(rows) == ['P1', 'P2']
    assert (rows['P1'].category, rows['P1'].title) == ('Books', 'Soap (book)')


def test_transform_breaks_rating_ties_by_category(spark):
    rows = transform(
        spark,
        [
            silver_product(category='Books'),
            silver_product(category='All_Beauty'),
            silver_product(category='Toys'),
        ],
    )

    assert rows['P1'].category == 'Toys'


def test_transform_empty_or_missing_categories_give_null_path(spark):
    rows = transform(
        spark,
        [
            silver_product(parent_asin='P1', categories=[]),
            silver_product(parent_asin='P2', categories=None),
        ],
    )

    assert rows['P1'].category_path is None
    assert rows['P2'].category_path is None


def test_transform_trims_store_and_turns_blank_into_null(spark):
    rows = transform(
        spark,
        [
            silver_product(parent_asin='P1', store=' Acme '),
            silver_product(parent_asin='P2', store=''),
            silver_product(parent_asin='P3', store='   '),
            silver_product(parent_asin='P4', store=None),
        ],
    )

    assert rows['P1'].store == 'Acme'
    assert [rows[p].store for p in ('P2', 'P3', 'P4')] == [None, None, None]
