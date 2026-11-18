import os
from pathlib import Path

import pytest
from airflow.models import DagBag

os.environ.setdefault('SPARK_BACKEND', 'docker')
os.environ.setdefault('HOST_PROJECT_DIR', '/tmp/amazon_reviews')

DAGS_DIR = Path(__file__).parents[2] / 'dags'


@pytest.fixture(scope='session', params=['docker', 'glue'])
def dag_bag(request) -> DagBag:
    # The backend is read when DAG files are parsed, so it only has to be set for this
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv('SPARK_BACKEND', request.param)
        return DagBag(dag_folder=str(DAGS_DIR))
