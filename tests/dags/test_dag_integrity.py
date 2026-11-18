EXPECTED_DAG_IDS = {'reviews_ingest', 'products', 'reviews_monthly'}


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_expected_dags_are_loaded(dag_bag):
    assert EXPECTED_DAG_IDS <= set(dag_bag.dag_ids)
