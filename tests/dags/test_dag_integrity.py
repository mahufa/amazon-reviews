from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).parents[2] / 'dags'
EXPECTED_DAG_IDS = {'reviews_ingest', 'products', 'reviews_monthly'}


@pytest.fixture(scope='session')
def dag_bag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_expected_dags_are_loaded(dag_bag):
    assert EXPECTED_DAG_IDS <= set(dag_bag.dag_ids)
