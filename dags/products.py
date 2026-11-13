from airflow.sdk import dag, task
from pendulum import datetime, duration

from include.helpers.ingest import get_and_store_file, get_categories
from include.helpers.paths import (
    GOLD_DIM_PRODUCT_TABLE,
    GOLD_PREFIX,
    HF_RAW_URL,
    LANDING_META_PREFIX,
    QUARANTINE_PREFIX,
    SILVER_PRODUCTS_TABLE,
)
from include.helpers.spark.job_factory import spark_job_depending_on_backend


@dag(
    dag_id='products',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    description='This DAG loads item metadata into silver products and gold dim_product.',
    tags=['products', 'amazon_reviews'],
    dagrun_timeout=duration(hours=5),
    default_args={
        'retries': 3,
        'retry_delay': duration(minutes=5),
    },
)
def products():
    @task
    def land_meta(category: str) -> str:
        return get_and_store_file(
            f'{HF_RAW_URL}/meta_categories/meta_{category}.jsonl',
            f'{LANDING_META_PREFIX}/meta_{category}.jsonl',
        )

    @task
    def join_uris(uris: list[str]) -> str:
        return ','.join(uris)

    meta_to_silver = spark_job_depending_on_backend(
        'meta_to_silver',
        'jobs/meta_to_silver.py',
        [
            '--sources',
            join_uris(land_meta.expand(category=get_categories())),
            '--table',
            SILVER_PRODUCTS_TABLE,
            '--quarantine-path',
            f's3a://{{{{ var.value.bucket_bronze }}}}/{QUARANTINE_PREFIX}/meta',
        ],
    )
    products_to_gold = spark_job_depending_on_backend(
        'products_to_gold',
        'jobs/products_to_gold.py',
        [
            '--source',
            SILVER_PRODUCTS_TABLE,
            '--table',
            GOLD_DIM_PRODUCT_TABLE,
            '--location',
            f's3://{{{{ var.value.bucket_gold }}}}/{GOLD_PREFIX}/dim_product',
        ],
    )

    meta_to_silver >> products_to_gold


products()
