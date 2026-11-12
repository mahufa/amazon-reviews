import argparse

from pyspark.sql import Column, DataFrame, SparkSession


def parse_args(args: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    for arg in args:
        parser.add_argument(arg, required=True)

    known_args, _ = parser.parse_known_args()
    return known_args


def epoch_ms_to_ts(epoch_ms: Column) -> Column:
    """Epoch millis (number or numeric string, fractions allowed) -> TIMESTAMP; NULL if invalid.

    Shared by bronze (partitioning) and silver (parsing) so both layers accept the same values.
    """
    return (epoch_ms.try_cast('decimal(20, 3)') / 1000).try_cast('timestamp')


def ensure_namespace(table: str, spark: SparkSession) -> None:
    namespace = table.rsplit('.', 1)[0]
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {namespace}')


def write_to_silver(
    df: DataFrame,
    table: str,
    partition_cols: list[str],
    replace_where: Column | None = None,
) -> None:
    writer = df.hint('rebalance', *partition_cols).writeTo(table)
    if not df.sparkSession.catalog.tableExists(table):
        writer.using('iceberg').partitionedBy(*partition_cols).create()
    elif replace_where is None:
        writer.overwritePartitions()
    else:
        writer.overwrite(replace_where)
