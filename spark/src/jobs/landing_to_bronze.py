import argparse

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession


def main() -> None:
    args = _parse_args()
    spark = (
        SparkSession.builder.appName('landing_to_bronze')
        .config('spark.sql.adaptive.advisoryPartitionSizeInBytes', '512m')
        .getOrCreate()
    )

    raw_reviews = spark.read.text(args.source)

    valid, invalid = _transform(raw_reviews, args.category)

    _write_valid(valid, args.target)
    _write_invalid(invalid, args.quarantine_path, args.category)

    spark.stop()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--source', required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--quarantine-path', required=True)
    parser.add_argument('--category', required=True)
    return parser.parse_args()


def _transform(
    raw: DataFrame,
    category: str,
) -> tuple[DataFrame, DataFrame]:
    ts = _str_to_ts()

    bronze = raw.select(
        'value',
        F.year(ts).alias('year'),
        F.month(ts).alias('month'),
        F.lit(category).alias('category'),
    ).cache()

    valid = bronze.filter(_is_valid())
    invalid = bronze.filter(~_is_valid()).select('value')

    return valid, invalid


def _write_valid(
    valid_df: DataFrame,
    target: str,
) -> None:
    (
        valid_df.hint('rebalance', 'year', 'month')
        .write.mode('overwrite')
        .option('partitionOverwriteMode', 'dynamic')
        .partitionBy('year', 'month', 'category')
        .text(target, 'gzip')
    )


def _write_invalid(
    invalid_df: DataFrame,
    quarantine_path: str,
    category: str,
) -> None:
    invalid_df.write.mode('overwrite').text(f'{quarantine_path}/category={category}', 'gzip')


def _str_to_ts() -> Column:
    ts = F.get_json_object('value', '$.timestamp')
    return (ts.try_cast('decimal(20, 3)') / 1000).try_cast('timestamp')


def _is_valid() -> Column:
    return F.col('year').isNotNull() & F.col('month').isNotNull()


if __name__ == '__main__':
    main()
