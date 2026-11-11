from airflow.sdk import Variable, dag, task, task_group
from pendulum import datetime, duration

from include.helpers.ingest import get_and_store_file
from include.helpers.paths import (
    BRONZE_REVIEWS_PREFIX,
    LANDING_META_PREFIX,
    LANDING_REVIEWS_PREFIX,
    QUARANTINE_PREFIX,
    SILVER_PRODUCTS_TABLE,
)
from include.helpers.spark.job_factory import spark_job_depending_on_backend

HF_RAW_URL = 'https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw'


@dag(
    dag_id='reviews_ingest',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    description='This DAG ingests raw reviews and item metadata.',
    tags=['ingest', 'amazon_reviews'],
    dagrun_timeout=duration(hours=5),
    default_args={
        'retries': 3,
        'retry_delay': duration(minutes=5),
    },
)
def reviews_ingest():
    @task
    def get_categories() -> list[str]:
        return Variable.get('reviews_categories', deserialize_json=True)

    @task
    def land_reviews(category: str) -> str:
        return get_and_store_file(
            f'{HF_RAW_URL}/review_categories/{category}.jsonl',
            f'{LANDING_REVIEWS_PREFIX}/{category}.jsonl',
        )

    @task
    def land_meta(category: str) -> str:
        return get_and_store_file(
            f'{HF_RAW_URL}/meta_categories/meta_{category}.jsonl',
            f'{LANDING_META_PREFIX}/meta_{category}.jsonl',
        )

    @task
    def join_uris(uris: list[str]) -> str:
        return ','.join(uris)

    @task_group
    def ingest_reviews(category: str):
        spark_job_depending_on_backend(
            'landing_to_bronze',
            'jobs/landing_to_bronze.py',
            [
                '--source',
                land_reviews(category),
                '--target',
                f's3a://{{{{ var.value.bucket_bronze }}}}/{BRONZE_REVIEWS_PREFIX}',
                '--quarantine-path',
                f's3a://{{{{ var.value.bucket_bronze }}}}/{QUARANTINE_PREFIX}/reviews',
                '--category',
                category,
            ],
        )

    categories = get_categories()
    ingest_reviews.expand(category=categories)

    spark_job_depending_on_backend(
        'meta_to_silver',
        'jobs/meta_to_silver.py',
        [
            '--sources',
            join_uris(land_meta.expand(category=categories)),
            '--table',
            SILVER_PRODUCTS_TABLE,
            '--quarantine-path',
            f's3a://{{{{ var.value.bucket_bronze }}}}/{QUARANTINE_PREFIX}/meta',
        ],
    )


reviews_ingest()
