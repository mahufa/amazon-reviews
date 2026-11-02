import pendulum
from airflow.sdk import dag, task, task_group
from pendulum import datetime, duration

from include.helpers.monthly import list_bronze_categories
from include.helpers.spark.job_factory import spark_job_depending_on_backend


@dag(
    dag_id='reviews_monthly',
    start_date=datetime(2022, 5, 1),
    end_date=datetime(2023, 9, 30),
    catchup=True,
    description='This DAG transforms raw reviews.',
    tags=['monthly_transform', 'amazon_reviews'],
    dagrun_timeout=duration(hours=5),
)
def reviews_monthly():
    @task
    def check_partition(data_interval_start: pendulum.DateTime | None = None) -> list[str]:
        return list_bronze_categories(data_interval_start)

    @task_group
    def transform_category(category: str):
        spark_job_depending_on_backend(
            'bronze_to_silver',
            'jobs/bronze_to_silver.py',
            [
                '--category',
                category,
                '--year',
                '{{ data_interval_start.year }}',
                '--month',
                '{{ data_interval_start.month }}',
                '--source',
                's3a://{{ var.value.bucket_bronze }}/amazon_reviews/reviews',
            ],
        ) >> spark_job_depending_on_backend(
            'silver_to_gold',
            'jobs/silver_to_gold.py',
            [
                '--category',
                category,
                '--year',
                '{{ data_interval_start.year }}',
                '--month',
                '{{ data_interval_start.month }}',
            ],
        )

    transform_category.expand(category=check_partition())


reviews_monthly()
