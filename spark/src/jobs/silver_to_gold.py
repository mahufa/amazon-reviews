import pyspark.sql.functions as F
from common import ensure_namespace, month_filter, parse_args, write_iceberg
from pyspark.sql import DataFrame, SparkSession

ARGS = [
    '--year',
    '--month',
    '--source',
    '--table',
    '--location',
]
PARTITION_COLUMNS = ['year', 'month', 'category']


def main() -> None:
    args = parse_args(ARGS)
    spark = (
        SparkSession.builder.appName('silver_to_gold')
        .config('spark.sql.adaptive.advisoryPartitionSizeInBytes', '512m')
        .getOrCreate()
    )

    ensure_namespace(args.table, spark)
    this_month = month_filter(int(args.year), int(args.month))

    fact_review = _transform(spark.table(args.source).filter(this_month))

    write_iceberg(fact_review, args.table, PARTITION_COLUMNS, this_month, args.location)

    spark.stop()


def _transform(reviews: DataFrame) -> DataFrame:
    return reviews.select(
        F.date_format('timestamp', 'yyyyMMdd').cast('int').alias('date_key'),
        F.col('timestamp').alias('review_ts'),
        'parent_asin',
        'asin',
        'user_id',
        'rating',
        'helpful_vote',
        'verified_purchase',
        F.length('text').alias('text_length'),
        *PARTITION_COLUMNS,
    )


if __name__ == '__main__':
    main()
