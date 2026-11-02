from airflow.sdk import Variable, dag, task, task_group
from pendulum import datetime, duration

from include.helpers.ingest import get_and_store_file
from include.helpers.spark.job_factory import spark_job_depending_on_backend

REVIEWS_DIR_PATH = 'https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/raw/review_categories/'


@dag(
    dag_id='reviews_ingest',
    start_date=datetime(2022, 1, 1),
    catchup=False,
    description='This DAG ingests raw reviews.',
    tags=['ingest', 'amazon_reviews'],
    dagrun_timeout=duration(hours=5),
)
def reviews_ingest():
    @task
    def get_categories() -> list[str]:
        return Variable.get('reviews_categories', deserialize_json=True)

    @task(
        retries=3,
        retry_delay=duration(minutes=5),
    )
    def land_data(reviews_category: str) -> str:
        category_path = REVIEWS_DIR_PATH + reviews_category + '.jsonl'
        s3_key = 'amazon_reviews/review_categories/' + reviews_category + '.jsonl'

        get_and_store_file(category_path, s3_key)

        return f's3a://{Variable.get("bucket_landing")}/{s3_key}'

    @task_group
    def ingest_and_partition_category(reviews_category: str):
        source_uri = land_data(reviews_category)
        spark_job_depending_on_backend(
            'landing_to_bronze',
            'jobs/landing_to_bronze.py',
            [
                '--source',
                source_uri,
                '--target',
                's3a://{{ var.value.bucket_bronze }}/amazon_reviews/reviews',
            ],
        )

    ingest_and_partition_category.expand(reviews_category=get_categories())


reviews_ingest()
