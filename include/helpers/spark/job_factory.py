import os
from pathlib import Path

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from include.helpers.spark.conf import spark_conf, to_conf_flags, to_glue_conf

LOCAL_SPARK_POOL = 'spark_local'
# Must not exceed the Glue jobs' MaxConcurrentRuns, or extra runs fail at start
GLUE_MAX_CONCURRENT_RUNS = 4


def spark_job_depending_on_backend(
    task_id: str,
    script: str,
    args: list[str],
) -> DockerOperator | GlueJobOperator:
    backend = os.environ.get('SPARK_BACKEND')
    if backend not in ['docker', 'glue']:
        raise ValueError(f'Backend {backend} not supported')

    conf = spark_conf(backend)
    if backend == 'docker':
        return _get_docker_op(task_id, script, conf, args)

    return _get_glue_op(task_id, script, conf, args)


def _get_docker_op(
    task_id: str,
    script: str,
    conf: dict[str, str],
    args: list[str],
) -> DockerOperator:
    env = {
        'AWS_ACCESS_KEY_ID': '{{ conn.lake_s3.login }}',
        'AWS_SECRET_ACCESS_KEY': '{{ conn.lake_s3.password }}',
        'AWS_REGION': '{{ conn.lake_s3.extra_dejson.region_name }}',
    }
    return DockerOperator(
        image='amazon-reviews-spark:latest',
        task_id=task_id,
        retries=1,
        pool=LOCAL_SPARK_POOL,
        command=[
            '/opt/spark/bin/spark-submit',
            '--master',
            'local[*]',
            *to_conf_flags(conf),
            script,
            *args,
        ],
        auto_remove='success',
        network_mode='default_net',
        mount_tmp_dir=False,
        environment=env,
        mounts=[
            Mount(
                source=f'{os.environ["HOST_PROJECT_DIR"]}/spark/src',
                target='/opt/app',
                type='bind',
                read_only=True,
            )
        ],
    )


def _get_glue_op(
    task_id: str,
    script: str,
    conf: dict[str, str],
    args: list[str],
) -> GlueJobOperator:
    script_args = dict(zip(args[::2], args[1::2], strict=True)) | {'--conf': to_glue_conf(conf)}

    return GlueJobOperator(
        task_id=task_id,
        job_name=_glue_job_name(script),
        retries=1,
        max_active_tis_per_dag=GLUE_MAX_CONCURRENT_RUNS,
        aws_conn_id='lake_s3',
        script_args=script_args,
        stop_job_run_on_kill=True,
        verbose=True,
        # Durable mode treats a cleared task as a retry and reuses the old succeeded run,
        # so reruns silently do nothing. Jobs are idempotent, so resubmitting is safe.
        durable=False,
    )


def _glue_job_name(script: str) -> str:
    return f'amazon-reviews-{Path(script).stem}'
