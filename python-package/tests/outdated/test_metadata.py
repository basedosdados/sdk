"""
Tests for metadata download fuunctions
"""
import shutil
from pathlib import Path

import pandas as pd
import pytest

from basedosdados import (
    get_dataset_description,
    get_table_columns,
    get_table_description,
    get_table_size,
    list_dataset_tables,
    list_datasets,
    search,
)
from basedosdados.download.metadata import _safe_fetch

TEST_PROJECT_ID = "basedosdados-dev"
SAVEFILE = Path(__file__).parent / "tmp_bases" / "test.csv"
SAVEPATH = Path(__file__).parent / "tmp_bases"
shutil.rmtree(SAVEPATH, ignore_errors=True)


def test_list_datasets_simple_verbose(capsys):
    """
    Test if list_datasets function works with verbose=True
    """

    out = list_datasets(with_description=False, verbose=True)
    out, _ = capsys.readouterr()  # Capture prints
    assert "dataset_id" in out


def test_list_datasets_simple_list():
    """
    Test if list_datasets function works with verbose=False
    """

    out = list_datasets(with_description=False, verbose=False)
    # check if function returns list
    assert isinstance(out, list)
    # check if list all datasets in API
    assert len(out) >= 84


def test_list_datasets_complete_list():
    """
    Test if list_datasets function works with verbose=False and with_description=True
    """

    out = list_datasets(with_description=True, verbose=False)
    # check if function returns list
    assert isinstance(out, list)
    assert "dataset_id" in out[0].keys()
    assert "description" in out[0].keys()


def test_list_datasets_complete_verbose(capsys):
    """
    Test list_datasets with complete output
    """

    list_datasets(with_description=True, verbose=True)
    out, _ = capsys.readouterr()  # Capture prints
    assert "dataset_id" in out
    assert "description" in out


def test_list_dataset_tables_simple_verbose(capsys):
    """
    Test list_dataset_tables function with verbose=True and with_description=False
    """

    list_dataset_tables(dataset_id="br_me_caged", with_description=False, verbose=True)
    out, _ = capsys.readouterr()  # Capture prints
    assert "table_id" in out


def test_list_dataset_tables_simple_list():
    """
    Test list_dataset_tables function with verbose=False and with_description=False
    """

    out = list_dataset_tables(
        dataset_id="br_me_caged", with_description=False, verbose=False
    )

    assert isinstance(out, list)
    assert len(out) > 0


def test_list_dataset_tables_complete_verbose(capsys):
    """
    Test list_dataset_tables function with verbose=True and with_description=True
    """

    list_dataset_tables(dataset_id="br_me_caged", with_description=True, verbose=True)

    out, _ = capsys.readouterr()  # Capture prints
    assert "table_id" in out
    assert "description" in out


def test_list_dataset_tables_complete_list():
    """
    Test list_dataset_tables function with verbose=False and with_description=True
    """

    out = list_dataset_tables(
        dataset_id="br_me_caged", with_description=True, verbose=False
    )

    assert isinstance(out, list)
    assert isinstance(out[0], dict)


def test_get_dataset_description(capsys):
    """
    Test get_dataset_description function with verbose=False
    """

    get_dataset_description("br_me_caged", verbose=True)
    out, _ = capsys.readouterr()  # Capture prints
    assert len(out) > 0


def test_get_dataset_description_verbose_false():
    """
    Test get_dataset_description function with verbose=False
    """
    out = get_dataset_description("br_me_caged", verbose=False)
    assert isinstance(out, str)
    assert len(out) > 0


def test_get_table_description(capsys):
    """
    Test get_table_description function with verbose=False
    """
    get_table_description("br_me_caged", "microdados_antigos")
    out, _ = capsys.readouterr()  # Capture prints
    assert len(out) > 0


def test_get_table_description_verbose_false():
    """
    Test get_table_description function with verbose=False
    """
    out = get_table_description(
        dataset_id="br_me_caged",
        table_id="microdados_antigos",
        verbose=False,
    )
    assert isinstance(out, str)
    assert len(out) > 0


def test_get_table_columns(capsys):
    """
    Test get_table_columns function with verbose=False
    """
    get_table_columns(
        dataset_id="br_me_caged",
        table_id="microdados_antigos",
    )
    out, _ = capsys.readouterr()  # Capture prints
    assert "name" in out
    assert "description" in out


def test_get_table_columns_verbose_false():
    """
    Test get_table_columns function with verbose=False
    """
    out = get_table_columns(
        dataset_id="br_me_caged",
        table_id="microdados_antigos",
        verbose=False,
    )
    assert isinstance(out, list)
    assert len(out) > 0


def test_search():
    """
    Test search function with verbose=False
    """
    out = search(query="agua", order_by="score")
    # check if function returns pd.DataFrame
    assert isinstance(out, pd.DataFrame)
    # check if there is duplicate tables in the result
    assert out.id.nunique() == out.shape[0]
    # check input error
    with pytest.raises(ValueError):
        search(query="agua", order_by="name")


def test_get_table_size(capsys):
    """
    Test get_table_size function with verbose=False
    """
    get_table_size(
        dataset_id="br_ibge_censo_demografico",
        table_id="setor_censitario_basico_2010",
    )
    out, _ = capsys.readouterr()
    assert "not available" in out


def test__safe_fetch(capsys):
    """
    Test _safe_fetch function with verbose=False
    """
    _safe_fetch("https://www.lkajsdhgfal.com.br")
    out, _ = capsys.readouterr()  # Capture prints
    assert "HTTPSConnection" in out

    response = _safe_fetch(
        "https://basedosdados.org/api/3/action/bd_dataset_search?q=agua&page_size=10&resource_type=bdm_table"
    )
    assert isinstance(response.json(), dict)
