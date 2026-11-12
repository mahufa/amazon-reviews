import pyspark.sql.functions as F
from common import ensure_namespace, parse_args, write_to_silver
from pyspark.sql import Column, DataFrame, SparkSession

ARGS = [
    '--sources',
    '--table',
    '--quarantine-path',
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
    spark = (
        SparkSession.builder.appName('meta_to_silver')
        .config('spark.sql.adaptive.advisoryPartitionSizeInBytes', '512m')
        .getOrCreate()
    )

    ensure_namespace(args.table, spark)

    paths = args.sources.split(',')

    raw_products = spark.read.text(paths).select(
        'value',
        _extract_cat().alias('category'),
    )

    products, corrupt = _transform(raw_products)

    write_to_silver(products, args.table, ['category'])
    _write_corrupt(corrupt, args.quarantine_path)

    spark.stop()


def _extract_cat() -> Column:
    return F.regexp_extract(F.col('_metadata.file_name'), r'^meta_(.+)\.jsonl$', 1)


def _transform(
    raw_products: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    parsed = raw_products.select(
        'value',
        'category',
        F.from_json(
            'value',
            SCHEMA,
            {'mode': 'PERMISSIVE', 'columnNameOfCorruptRecord': '_corrupt_record'},
        ).alias('j'),
    ).cache()

    valid = (
        parsed.filter(_is_valid())
        .select('j.*', 'category')
        .drop('_corrupt_record')
        .withColumn('price', F.col('price').try_cast('decimal(10,2)'))
        .dropDuplicates(['parent_asin', 'category'])
    )
    corrupt = parsed.filter(~_is_valid()).select('value', 'category')

    return valid, corrupt


def _write_corrupt(
    invalid_df: DataFrame,
    quarantine_path: str,
) -> None:
    (
        invalid_df.write.mode('overwrite')
        .partitionBy('category')
        .option('partitionOverwriteMode', 'static')
        .text(quarantine_path, 'gzip')
    )


def _is_valid() -> Column:
    return F.col('j._corrupt_record').isNull() & F.col('j.parent_asin').isNotNull()


if __name__ == '__main__':
    main()
