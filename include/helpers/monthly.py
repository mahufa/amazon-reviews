from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable
from airflow.sdk.exceptions import AirflowSkipException


def list_bronze_categories(
    data_interval_start,
) -> list[str]:
    prefix = f'amazon_reviews/reviews/year={data_interval_start:%Y}/month={data_interval_start:%m}/'
    prefixes = S3Hook(aws_conn_id='lake_s3').list_prefixes(
        bucket_name=Variable.get('bucket_bronze'),
        prefix=prefix,
        delimiter='/',
    )
    if not prefixes:
        raise AirflowSkipException(f'No data for {prefix}')

    return [p.rstrip('/').split('category=')[1] for p in prefixes]
