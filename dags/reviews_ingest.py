from airflow.sdk import dag, task, task_group
from pendulum import datetime, duration

from include.helpers.ingest import get_and_store_file, get_categories
from include.helpers.paths import (
    BRONZE_REVIEWS_PREFIX,
    HF_RAW_URL,
    LANDING_REVIEWS_PREFIX,
    QUARANTINE_PREFIX,
)
from include.helpers.spark.job_factory import spark_job_depending_on_backend


@dag(
    dag_id='reviews_ingest',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    description='This DAG ingests raw reviews into bronze.',
    tags=['ingest', 'amazon_reviews'],
    dagrun_timeout=duration(hours=5),
    default_args={
        'retries': 3,
        'retry_delay': duration(minutes=5),
    },
)
def reviews_ingest():
    @task
    def land_reviews(category: str) -> str:
        return get_and_store_file(
            f'{HF_RAW_URL}/review_categories/{category}.jsonl',
            f'{LANDING_REVIEWS_PREFIX}/{category}.jsonl',
        )

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

    ingest_reviews.expand(category=get_categories())


reviews_ingest()
