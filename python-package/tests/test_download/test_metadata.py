from basedosdados.backend import Backend
from basedosdados.download import metadata


def test_get_datasets_output():
    """
    Test if gets datasets correct output type
    """

    backend = Backend()

    out = metadata.get_datasets(dataset_id="", backend=backend)
    assert isinstance(out, dict)

    out = metadata.get_datasets(dataset_id="", backend=backend)
    assert isinstance(out, dict)


def test_get_datasets_output_with_input():
    """
    Test if gets datasets output with an input
    """

    backend = Backend()

    out = metadata.get_datasets(dataset_name="br_me_caged", backend=backend)
    ## out: {'items': [], 'page': 1, 'page_size': 10, 'page_total': 0}
    assert isinstance(out, dict)
    assert len(out) > 0


def test_get_datasets_size():
    """
    Test if gets datasets correct output length
    """

    backend = Backend()

    out = metadata.get_datasets(dataset_id="", backend=backend)
    assert len(out) > 0
    assert out["page_total"] > 100


def test_get_tables_output():
    """
    Test if gets tables correct output type
    """

    backend = Backend()

    out = metadata.get_tables(dataset_id="", backend=backend)
    assert isinstance(out, dict)


def test_get_tables_output_with_input():
    """
    Test if gets tables output with an input
    """

    backend = Backend()

    out = metadata.get_tables(table_name="br_me_caged", backend=backend)
    ## out: {'items': [], 'page': 1, 'page_size': 10, 'page_total': 0}
    assert isinstance(out, dict)
    assert len(out) > 0


def test_get_tables_size():
    """
    Test if gets tables correct output length
    """

    backend = Backend()

    out = metadata.get_tables(dataset_id="", backend=backend)
    assert len(out) > 0
    assert out["page_total"] > 50


def test_get_columns_output():
    """
    Test if gets columns correct output type
    """

    backend = Backend()

    out = metadata.get_columns(table_id="", backend=backend)
    assert isinstance(out, dict)


def test_get_columns_output_with_input():
    """
    Test if gets columns output with an input
    """

    backend = Backend()

    out = metadata.get_columns(columns_name="microdados_antigos", backend=backend)
    ## out: {'items': [], 'page': 1, 'page_size': 10, 'page_total': 0}
    assert isinstance(out, dict)
    assert len(out) > 0


def test_get_columns_size():
    """
    Test if gets columns correct output length
    """

    backend = Backend()

    out = metadata.get_columns(table_id="", backend=backend)
    assert len(out) > 0
    assert out["page_total"] > 3000


def test_search_output():
    """
    Test if gets search correct output type
    """

    backend = Backend()

    out = metadata.search(q="", backend=backend)
    assert isinstance(out, list)
    assert isinstance(out[0], dict)


def test_search_output_with_input():
    """
    Test if gets search output with an input
    """

    backend = Backend()

    out = metadata.search(q="data", backend=backend)
    assert isinstance(out, list)
    assert isinstance(out[0], dict)
    assert "slug" in out[0].keys()
    assert "name" in out[0].keys()
    assert "description" in out[0].keys()
    assert "n_tables" in out[0].keys()
    assert "n_raw_data_sources" in out[0].keys()
    assert "n_information_requests" in out[0].keys()
    assert "organization" in out[0].keys()


def test_search_size():
    """
    Test if gets search correct output length
    """

    backend = Backend()

    out = metadata.search(q="", backend=backend)
    assert len(out) > 0
