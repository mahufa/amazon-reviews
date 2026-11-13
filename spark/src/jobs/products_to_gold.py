import pyspark.sql.functions as F
from common import ensure_namespace, parse_args
from pyspark.sql import DataFrame, SparkSession

ARGS = [
    '--source',
    '--table',
    '--location',
]


def main() -> None:
    args = parse_args(ARGS)
    spark = (
        SparkSession.builder.appName('products_to_gold')
        # No rebalance here: the groupBy shuffle sets the file count. Without this AQE
        # keeps one partition per core (10 tiny files) instead of the advisory size
        .config('spark.sql.adaptive.coalescePartitions.parallelismFirst', 'false')
        .getOrCreate()
    )

    ensure_namespace(args.table, spark)

    dim_product = _transform(spark.table(args.source))

    _write(dim_product, args.table, args.location)

    spark.stop()


def _transform(products: DataFrame) -> DataFrame:
    """One row per `parent_asin` (silver has one per `parent_asin` + `category`).

    The listing with the most ratings wins; ties go to the last category name, so the
    choice is deterministic. A NULL `rating_number` loses to any number.
    """
    attributes = F.struct(
        'title',
        F.nullif(F.trim('store'), F.lit('')).alias('store'),
        'main_category',
        'category',
        F.nullif(F.array_join('categories', ' > '), F.lit('')).alias('category_path'),
        'price',
        'average_rating',
        'rating_number',
    )
    rank = F.struct(F.coalesce(F.col('rating_number'), F.lit(-1)), F.col('category'))

    return (
        products.groupBy('parent_asin')
        .agg(F.max_by(attributes, rank).alias('p'))
        .select('parent_asin', 'p.*')
    )


def _write(dim_product: DataFrame, table: str, location: str) -> None:
    (
        dim_product.writeTo(table)
        .using('iceberg')
        .tableProperty('location', location)
        .createOrReplace()
    )


if __name__ == '__main__':
    main()
