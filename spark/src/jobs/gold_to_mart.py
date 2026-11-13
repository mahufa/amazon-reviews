import pyspark.sql.functions as F
from common import ensure_namespace, month_filter, parse_args, write_iceberg
from pyspark.sql import DataFrame, SparkSession

ARGS = [
    '--year',
    '--month',
    '--fact',
    '--dim-product',
    '--table',
    '--location',
]
PARTITION_COLUMNS = ['year', 'month']
GRAIN = [*PARTITION_COLUMNS, 'category', 'store']


def main() -> None:
    args = parse_args(ARGS)
    spark = (
        SparkSession.builder.appName('gold_to_mart')
        .config('spark.sql.autoBroadcastJoinThreshold', '256m')
        .getOrCreate()
    )

    ensure_namespace(args.table, spark)
    this_month = month_filter(int(args.year), int(args.month))

    mart = _transform(
        spark.table(args.fact).filter(this_month),
        spark.table(args.dim_product),
    )

    write_iceberg(mart, args.table, PARTITION_COLUMNS, this_month, args.location)

    spark.stop()


def _transform(fact_review: DataFrame, dim_product: DataFrame) -> DataFrame:
    """Monthly ratings per store and category.

    Left join: reviews of products missing from `dim_product`, or with no store, are kept
    under a NULL store. `avg_rating` stays unrounded so it can be re-weighted by `review_count`.
    """
    stores = dim_product.select('parent_asin', 'store')

    return (
        fact_review.join(stores, 'parent_asin', 'left')
        .groupBy(*GRAIN)
        .agg(
            F.count('*').alias('review_count'),
            F.avg('rating').alias('avg_rating'),
            F.avg(F.col('verified_purchase').cast('int')).alias('verified_share'),
            F.coalesce(F.sum('helpful_vote'), F.lit(0)).alias('helpful_votes'),
            F.count_distinct('parent_asin').alias('product_count'),
        )
    )


if __name__ == '__main__':
    main()
