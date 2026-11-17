import pytest
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from include.helpers.spark.conf import to_glue_conf
from include.helpers.spark.job_factory import GLUE_MAX_CONCURRENT_RUNS


def test_to_glue_conf_chains_settings():
    # Glue prefixes the value with --conf itself, so only later settings get the flag
    assert to_glue_conf({'a': '1', 'b': '2'}) == 'a=1 --conf b=2'


@pytest.mark.parametrize('dag_bag', ['glue'], indirect=True)
def test_spark_tasks_are_glue_jobs(dag_bag):
    task = dag_bag.dags['reviews_monthly'].get_task('bronze_to_silver')

    assert isinstance(task, GlueJobOperator)
    assert task.job_name == 'amazon-reviews-bronze_to_silver'
    assert task.aws_conn_id == 'lake_s3'
    assert task.max_active_tis_per_dag == GLUE_MAX_CONCURRENT_RUNS
    assert task.script_args['--year'] == '{{ data_interval_start.year }}'
    assert 'spark.sql.catalog.lake.type=glue' in task.script_args['--conf']
