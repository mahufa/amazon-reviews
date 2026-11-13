import pendulum
from airflow.sdk import dag, task
from pendulum import datetime, duration

from include.helpers.monthly import bronze_month_has_data
from include.helpers.paths import (
    BRONZE_REVIEWS_PREFIX,
    GOLD_FACT_REVIEW_TABLE,
    GOLD_PREFIX,
    SILVER_REJECTS_TABLE,
    SILVER_REVIEWS_TABLE,
)
from include.helpers.spark.job_factory import spark_job_depending_on_backend


@dag(
    dag_id='reviews_monthly',
    start_date=datetime(2022, 5, 1),
    end_date=datetime(2023, 9, 30),
    catchup=True,
    description='This DAG transforms raw reviews.',
    tags=['monthly_transform', 'amazon_reviews'],
    dagrun_timeout=duration(hours=5),
    schedule='@monthly',
    max_active_runs=1,
)
def reviews_monthly():
    @task.short_circuit
    def check_partition(data_interval_start: pendulum.DateTime | None = None) -> bool:
        return bronze_month_has_data(data_interval_start)

    bronze_to_silver = spark_job_depending_on_backend(
        'bronze_to_silver',
        'jobs/bronze_to_silver.py',
        [
            '--year',
            '{{ data_interval_start.year }}',
            '--month',
            '{{ data_interval_start.month }}',
            '--source',
            f's3a://{{{{ var.value.bucket_bronze }}}}/{BRONZE_REVIEWS_PREFIX}',
            '--table',
            SILVER_REVIEWS_TABLE,
            '--rejects-table',
            SILVER_REJECTS_TABLE,
        ],
    )
    silver_to_gold = spark_job_depending_on_backend(
        'silver_to_gold',
        'jobs/silver_to_gold.py',
        [
            '--year',
            '{{ data_interval_start.year }}',
            '--month',
            '{{ data_interval_start.month }}',
            '--source',
            SILVER_REVIEWS_TABLE,
            '--table',
            GOLD_FACT_REVIEW_TABLE,
            '--location',
            f's3://{{{{ var.value.bucket_gold }}}}/{GOLD_PREFIX}/fact_review',
        ],
    )

    check_partition() >> bronze_to_silver >> silver_to_gold


reviews_monthly()
