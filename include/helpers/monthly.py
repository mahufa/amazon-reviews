from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable

from include.helpers.paths import BRONZE_REVIEWS_PREFIX


def bronze_month_has_data(
    data_interval_start,
) -> bool:
    prefix = (
        f'{BRONZE_REVIEWS_PREFIX}/'
        f'year={data_interval_start.year}/month={data_interval_start.month}/'
    )
    prefixes = S3Hook(aws_conn_id='lake_s3').list_prefixes(
        bucket_name=Variable.get('bucket_bronze'),
        prefix=prefix,
        delimiter='/',
    )

    return bool(prefixes)
