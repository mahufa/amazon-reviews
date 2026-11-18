import pyspark.sql.functions as F
from common import ensure_namespace, parse_args, write_iceberg
from pyspark.sql import Column, DataFrame, SparkSession

ARGS = [
    '--source',
    '--table',
    '--quarantine-path',
    '--category',
]
SCHEMA = """
    parent_asin STRING,
    title STRING,
    average_rating DOUBLE,
    rating_number INTEGER,
    price STRING,
    main_category STRING,
    store STRING,
    categories ARRAY<STRING>,
    details MAP<STRING, STRING>,
    _corrupt_record STRING
"""


def main() -> None:
    args = parse_args(ARGS)
    spark = SparkSession.builder.appName('meta_to_silver').getOrCreate()

    ensure_namespace(args.table, spark)

    raw_products = spark.read.text(args.source)

    products, corrupt = _transform(raw_products, args.category)

    write_iceberg(products, args.table, ['category'])
    _write_corrupt(corrupt, args.quarantine_path, args.category)

    spark.stop()


def _transform(
    raw_products: DataFrame,
    category: str,
) -> tuple[DataFrame, DataFrame]:
    parsed = raw_products.select(
        'value',
        F.from_json(
            'value',
            SCHEMA,
            {'mode': 'PERMISSIVE', 'columnNameOfCorruptRecord': '_corrupt_record'},
        ).alias('j'),
    )

    valid = (
        parsed.filter(_is_valid())
        .select('j.*', F.lit(category).alias('category'))
        .drop('_corrupt_record')
        .withColumn('price', F.col('price').try_cast('decimal(10,2)'))
        .dropDuplicates(['parent_asin', 'category'])
    )
    corrupt = parsed.filter(~_is_valid()).select('value')

    return valid, corrupt


def _write_corrupt(
    invalid_df: DataFrame,
    quarantine_path: str,
    category: str,
) -> None:
    invalid_df.write.mode('overwrite').text(f'{quarantine_path}/category={category}', 'gzip')


def _is_valid() -> Column:
    return F.col('j._corrupt_record').isNull() & F.col('j.parent_asin').isNotNull()


if __name__ == '__main__':
    main()
