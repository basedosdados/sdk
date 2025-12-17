"""
Tests for the `download` class.
"""

import shutil
from pathlib import Path

import pandas as pd
import pytest
from pandas_gbq.gbq import GenericGBQException

from basedosdados import download, read_sql, read_table
from basedosdados.exceptions import (
    BaseDosDadosAccessDeniedException,
    BaseDosDadosException,
)

TEST_PROJECT_ID = "basedosdados-dev"
SAVE_DIR = Path("tests") / "tmp"
SAVEFILE = SAVE_DIR / "test.csv"

shutil.rmtree(SAVE_DIR, ignore_errors=True)


def test_download_by_query():
    """
    Test for the `download` function when the query is provided.
    """

    download(
        SAVEFILE,
        query="select * from `basedosdados.br_ibge_pib.municipio` limit 10",
        billing_project_id=TEST_PROJECT_ID,
        from_file=True,
    )

    assert SAVEFILE.exists()


def test_download_by_table():
    """
    Test for the `download` function when the table and dataset ids are
    provided.
    """

    download(
        SAVEFILE,
        dataset_id="br_ibge_pib",
        table_id="municipio",
        billing_project_id=TEST_PROJECT_ID,
        limit=10,
        from_file=True,
    )

    assert SAVEFILE.exists()


def test_download_large_file():
    """
    Test for the `download` function for a large file when the query is
    provided.
    """

    download(
        SAVEFILE,
        query="select * from basedosdados.br_me_rais.microdados_vinculos limit 10",
        billing_project_id=TEST_PROJECT_ID,
        from_file=True,
    )

    assert (SAVEFILE).exists()


def test_download_no_query_or_table():
    """
    Test if the `download` function raises an error when neither the query nor
    the table are provided.
    """

    with pytest.raises(BaseDosDadosException):
        download(
            SAVEFILE,
            limit=10,
        )


def test_read_sql():
    """
    Tests if read_sql returns a pandas DataFrame.
    """

    assert isinstance(
        read_sql(
            query="select * from `basedosdados.br_ibge_pib.municipio` limit 10",
            billing_project_id=TEST_PROJECT_ID,
            from_file=True,
        ),
        pd.DataFrame,
    )


def test_read_sql_invalid_billing_project_id():
    """
    Test if the `read_sql` function raises an error when the billing project
    id is not valid.
    """

    with pytest.raises(GenericGBQException):
        read_sql(
            query="select * from `basedosdados.br_ibge_pib.municipio` limit 10",
            billing_project_id="inexistent_project_id",
            from_file=True,
        )


def test_read_sql_inexistent_project():
    """
    Test if the `read_sql` function raises an error when the billing project id
    is not valid.
    """

    with pytest.raises(BaseDosDadosAccessDeniedException):
        read_sql(
            query="select * from `asedosdados.br_ibge_pib.municipio` limit 10",
            billing_project_id=TEST_PROJECT_ID,
            from_file=True,
        )


def test_read_sql_inexistent_dataset():
    """
    Test if the `read_sql` function raises an error when the dataset id is not
    valid.
    """

    with pytest.raises(GenericGBQException) as excinfo:
        read_sql(
            query="select * from `basedosdados-dev.br_ibge_inexistent.municipio` limit 10",
            billing_project_id=TEST_PROJECT_ID,
            from_file=True,
        )

    assert "Reason: 404 POST" in str(excinfo.value)
    assert (
        "Not found: Dataset basedosdados-dev:br_ibge_inexistent was not found"
        in str(excinfo.value)
    )


def test_read_sql_inexistent_table():
    """
    Test if the `read_sql` function raises an error when the table id is not
    valid.
    """

    with pytest.raises(GenericGBQException) as excinfo:
        read_sql(
            query="select * from `basedosdados.br_ibge_pib.inexistent` limit 10",
            billing_project_id=TEST_PROJECT_ID,
            from_file=True,
        )

    assert "Reason: 404 POST" in str(excinfo.value)
    assert (
        "Not found: Table basedosdados:br_ibge_pib.inexistent was not found"
        in str(excinfo.value)
    )


def test_read_sql_syntax_error():
    """
    Test if the `read_sql` function raises an error when the query is not
    valid.
    """

    with pytest.raises(GenericGBQException) as excinfo:
        read_sql(
            query="invalid_statement * from `basedosdados.br_ibge_pib.municipio` limit 10",
            billing_project_id=TEST_PROJECT_ID,
            from_file=True,
        )

    assert "Reason: 400 POST" in str(excinfo.value)


def test_read_sql_out_of_bound_date():
    """
    Test if the `read_sql` function raises an error when the date is out of
    bound.
    """

    read_sql(
        query="select DATE('1000-01-01')",
        billing_project_id=TEST_PROJECT_ID,
        from_file=True,
    )


def test_read_table():
    """
    Tests if read_table returns a pandas DataFrame.
    """

    assert isinstance(
        read_table(
            dataset_id="br_ibge_pib",
            table_id="municipio",
            billing_project_id=TEST_PROJECT_ID,
            from_file=True,
            limit=10,
        ),
        pd.DataFrame,
    )
