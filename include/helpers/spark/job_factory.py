import os

from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

from include.helpers.spark.conf import spark_conf, to_conf_flags

LOCAL_SPARK_POOL = 'spark_local'


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
        # One retry for transient failures; code/schema errors would just fail again
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


# TODO: Glue 6.0 job operator (needs Terraform: script bucket, IAM role)
def _get_glue_op(
    task_id: str,
    script: str,
    conf: dict[str, str],
    args: list[str],
) -> GlueJobOperator:
    raise NotImplementedError
