import pyspark.sql.functions as F
from common import ensure_namespace, epoch_ms_to_ts, month_filter, parse_args, write_iceberg
from pyspark.sql import Column, DataFrame, SparkSession

ARGS = [
    '--year',
    '--month',
    '--source',
    '--table',
    '--rejects-table',
]
SCHEMA = """
    rating DOUBLE,
    title STRING,
    text STRING,
    asin STRING,
    parent_asin STRING,
    user_id STRING,
    timestamp STRING,
    helpful_vote INTEGER,
    verified_purchase BOOLEAN,
    _corrupt_record STRING
"""
PARTITION_COLUMNS = ['year', 'month', 'category']
DEDUP_KEYS = ['user_id', 'asin', 'timestamp', 'category']


def main() -> None:
    args = parse_args(ARGS)
    spark = SparkSession.builder.appName('bronze_to_silver').getOrCreate()

    ensure_namespace(args.table, spark)
    year, month = int(args.year), int(args.month)

    reviews_partition = _read_month(spark, args.source, year, month)

    reviews, rejects = _transform(reviews_partition)

    this_month = month_filter(year, month)
    write_iceberg(reviews, args.table, PARTITION_COLUMNS, this_month)
    write_iceberg(rejects, args.rejects_table, PARTITION_COLUMNS, this_month)

    spark.stop()


def _read_month(
    spark: SparkSession,
    source: str,
    year: int,
    month: int,
) -> DataFrame:
    return spark.read.option('basePath', source).text(f'{source}/year={year}/month={month}')


def _transform(
    reviews_partition: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    parsed = (
        reviews_partition.select(
            'value',
            *PARTITION_COLUMNS,
            F.from_json(
                'value',
                SCHEMA,
                options={
                    'mode': 'PERMISSIVE',
                    'columnNameOfCorruptRecord': '_corrupt_record',
                },
            ).alias('j'),
        )
        .withColumn('reason', _reject_reason())
        .cache()
    )

    valid = _dedup(
        parsed.filter(F.col('reason').isNull())
        .select(
            'j.*',
            *PARTITION_COLUMNS,
        )
        .drop('_corrupt_record')
        .withColumn('timestamp', epoch_ms_to_ts(F.col('timestamp')))
    )
    rejects = parsed.filter(F.col('reason').isNotNull()).select(
        'value',
        'reason',
        *PARTITION_COLUMNS,
    )

    return valid, rejects


def _dedup(reviews: DataFrame) -> DataFrame:
    others = [c for c in reviews.columns if c not in DEDUP_KEYS]
    return (
        reviews.groupBy(*DEDUP_KEYS)
        .agg(
            F.max_by(
                F.struct(*others),
                F.coalesce(F.col('helpful_vote'), F.lit(-1)),
            ).alias('r')
        )
        .select(*DEDUP_KEYS, 'r.*')
        .select(*reviews.columns)
    )


def _reject_reason() -> Column:
    """First failed rule per row; NULL means the row is valid."""
    j = F.col('j')
    return (
        F.when(j.isNull() | j['_corrupt_record'].isNotNull(), 'corrupt')
        .when(
            j['parent_asin'].isNull() | j['asin'].isNull() | j['user_id'].isNull(),
            'missing_key',
        )
        .when(~F.coalesce(j['rating'].between(1, 5), F.lit(False)), 'invalid_rating')
    )


if __name__ == '__main__':
    main()
