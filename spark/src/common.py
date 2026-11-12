import argparse

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, DataFrameWriterV2, SparkSession


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


def month_filter(year: int, month: int) -> Column:
    """Predicate on the `year`/`month` partition columns shared by silver and gold."""
    return (F.col('year') == year) & (F.col('month') == month)


def ensure_namespace(table: str, spark: SparkSession) -> None:
    namespace = table.rsplit('.', 1)[0]
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {namespace}')


def write_iceberg(
    df: DataFrame,
    table: str,
    partition_cols: list[str],
    replace_where: Column | None = None,
    location: str | None = None,
) -> None:
    writer = df.hint('rebalance', *partition_cols).writeTo(table)
    if not df.sparkSession.catalog.tableExists(table):
        _write_with_create(writer, partition_cols, location)
    elif replace_where is None:
        writer.overwritePartitions()
    else:
        writer.overwrite(replace_where)


def _write_with_create(
    writer: DataFrameWriterV2,
    partition_cols: list[str],
    location: str | None,
) -> None:
    writer = writer.using('iceberg').partitionedBy(*partition_cols)
    if location is not None:
        writer = writer.tableProperty('location', location)
    writer.create()
