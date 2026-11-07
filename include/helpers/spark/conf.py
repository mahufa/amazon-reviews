COMMON_CONF = {
    'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    'spark.sql.catalog.lake': 'org.apache.iceberg.spark.SparkCatalog',
    'spark.sql.catalog.lake.warehouse': 's3://{{ var.value.bucket_silver }}/',
    'spark.sql.catalog.lake.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
    'spark.sql.sources.partitionOverwriteMode': 'dynamic',
    'spark.sql.session.timeZone': 'UTC',
}

DOCKER_CONF = {
    'spark.sql.catalog.lake.type': 'jdbc',
    'spark.sql.catalog.lake.uri': 'jdbc:postgresql://postgres-airflow:5432/iceberg_catalog',
    'spark.sql.catalog.lake.jdbc.user': 'airflow',
    'spark.sql.catalog.lake.jdbc.password': 'airflow',
    'spark.sql.catalog.lake.s3.endpoint': '{{ conn.lake_s3.extra_dejson.endpoint_url }}',
    'spark.sql.catalog.lake.s3.path-style-access': 'true',
    'spark.hadoop.fs.s3a.endpoint': '{{ conn.lake_s3.extra_dejson.endpoint_url }}',
    'spark.hadoop.fs.s3a.path.style.access': 'true',
    'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
}

GLUE_CONF = {
    'spark.sql.catalog.lake.type': 'glue',
}

BACKEND_CONF = {'docker': DOCKER_CONF, 'glue': GLUE_CONF}


def spark_conf(backend: str) -> dict[str, str]:
    return COMMON_CONF | BACKEND_CONF[backend]


def to_conf_flags(conf: dict[str, str]) -> list[str]:
    return [part for key, value in conf.items() for part in ('--conf', f'{key}={value}')]
