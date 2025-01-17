import pytest

from basedosdados.exceptions import BaseDosDadosException
from basedosdados.upload.dataset import Dataset
from basedosdados.upload.table import Table

DATASET_ID = "pytest"
TABLE_ID = "pytest"

csv_path = "tests/test_upload/table/municipio.csv"
avro_path = "tests/test_upload/table/municipio.avro"
parquet_path = "tests/test_upload/table/municipio.parquet"
sql_path = "tests/test_upload/table/publish.sql"

table = Table(dataset_id=DATASET_ID, table_id=TABLE_ID)
table2 = Table(dataset_id="br_me_rais", table_id="microdados_estabelecimentos")


def test_table_config():
    """
    Test table config return
    """
    out = table2.table_config

    assert isinstance(out, dict)
    assert len(out) != 0


def test_is_partitioned():
    """
    Test if the sample is partitioned
    """
    out = table._is_partitioned(
        data_sample_path=csv_path, source_format="csv", csv_delimiter=","
    )

    assert isinstance(out, bool)
    assert out is False


@pytest.mark.skip(reason="incomplete test because of flow issues")
def test_load_schema_from_api():
    """
    Test if return the schema
    """
    out = table._load_schema_from_api(
        "staging"
    )  ## IndexError: list index out of range (backend.py:437)
    ## WARNING  | _load_schema_from_api:143 -  Table pytest allready exists, replacing schema!
    assert isinstance(out, list)


def test_get_table_description():
    """
    Test if get the correct description of a table
    """

    out = table._get_table_description("staging")

    assert isinstance(out, str)
    assert "staging table for `basedosdados-dev.pytest.pytest" in out


@pytest.mark.skip(reason="incomplete test because of flow issues")
def test_get_table_description_error(capsys):
    """
    Test the exception of table description
    """

    table._get_table_description("prod")
    _, out = capsys.readouterr()
    ## WARNING  | _get_table_description:478 - table pytest does not have a description in the API.
    assert "description not available in the API." in out


def test_create():
    """
    Test create method
    """

    table.create(
        path=csv_path,
        if_table_exists="pass",
        if_dataset_exists="pass",
        if_storage_data_exists="pass",
    )

    out = table.table_exists("staging")

    assert out is True


def test_load_schema_from_bq():
    """
    Test if return the schema
    """

    out = table._load_schema_from_bq("staging")

    assert isinstance(out, list)
    assert len(out) > 5


def test_append(capsys):
    """
    Test append method
    """

    table.append(csv_path)
    _, out = capsys.readouterr()

    assert "Table pytest was appended!" in out


def test_append_error():
    """
    Test if table not exists throw an exception
    """

    with pytest.raises(BaseDosDadosException):
        table.append(csv_path, if_exists="raise")


def test_create_table_exists():
    """
    Test create if table already exists
    """

    with pytest.raises(FileExistsError):
        table.create(
            path=csv_path,
            if_table_exists="raise",
            if_dataset_exists="pass",
            if_storage_data_exists="pass",
        )


def test_create_with_error():
    """
    Test if throw an error trying to create table
    """

    with pytest.raises(BaseDosDadosException):
        table.create(
            path=csv_path,
            if_table_exists="raise",
            if_dataset_exists="raise",
            if_storage_data_exists="raise",
        )


def test_get_columns_from_bq():
    """
    Test if get the columns
    """
    out = table._get_columns_from_bq("staging")

    assert isinstance(out, dict)
    assert len(out) != 0


@pytest.mark.skip(reason="incomplete test because of flow issues")
def test_get_columns_from_bq_prod(capsys):
    """
    Test if get the columns when is prod
    """
    table._get_columns_from_bq(
        "prod"
    )  ## TypeError: exceptions must derive from BaseException
    _, out = capsys.readouterr()
    ## ERROR    | _get_columns_from_bq:272 - Table pytest.pytest does not exist in prod, please create first!
    assert (
        "Table pytest.pytest does not exist in prod, please create first!"
        in out
    )


@pytest.mark.skip(reason="incomplete test because of flow issues")
def test_get_cross_columns_from_bq_api():
    """
    Test if get the bd_columns
    """

    out = (
        table._get_cross_columns_from_bq_api()
    )  ## IndexError: list index out of range (backend.py:437)
    assert isinstance(out, dict)


def test_get_columns_from_data():
    """
    Test if get the columns from data
    """
    out = table._get_columns_from_data(csv_path, mode="staging")
    out2 = table._get_columns_from_data(
        data_sample_path="tests/test_upload/table",
        source_format="csv",
        mode="staging",
    )

    assert isinstance(out, dict)
    assert isinstance(out2, dict)
    assert 10 > len(out["columns"]) > 0
    assert len(out2) != 0
    assert len(out) != 0


def test_create_dataset_and_storage_exists():
    """
    Test create if dataset and storage already exists
    """

    with pytest.raises(BaseDosDadosException):
        table.create(
            path=csv_path,
            if_table_exists="replace",
            if_dataset_exists="raise",
            if_storage_data_exists="raise",
        )


def test_create_no_path_error():
    """
    Teste if error is raised when no path is provided
    """

    Dataset(dataset_id=DATASET_ID).create(mode="staging", if_exists="pass")

    with pytest.raises(FileNotFoundError):
        table.create("dev-api", if_storage_data_exists="raise")

    with pytest.raises(FileNotFoundError):
        table.create("dev-api", if_dataset_exists="raise")


def test_table_create_with_parquet_source_format():
    """
    Test create when source format is parquet
    """

    table.create(
        path=parquet_path,
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="replace",
    )
    assert table.table_exists("staging")


def test_table_create_with_avro_source_format(capsys):
    """
    Test create when source format is avro
    """

    table.create(
        path=avro_path,
        source_format="avro",
        if_table_exists="pass",
        if_storage_data_exists="pass",
        if_dataset_exists="pass",
    )
    _, out = capsys.readouterr()
    assert "File municipio.avro_staging was uploaded!" in out


def test_table_create_not_implemented_source_format():
    """
    Test create when source format is not implemented
    """

    with pytest.raises(NotImplementedError):
        table.create(
            path=csv_path,
            if_table_exists="pass",
            if_storage_data_exists="pass",
            if_dataset_exists="pass",
            source_format="json",
        )


def test_delete_all():
    """
    Test delete method with all modes
    """
    table.create(
        path=csv_path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="replace",
        source_format="csv",
    )

    table.delete("all")

    assert not table.table_exists("staging")
    assert not table.table_exists("prod")


def test_delete_staging(capsys):
    """
    Test delete method with staging mode
    """
    table.create(
        path=csv_path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="replace",
        source_format="csv",
    )

    table.delete("staging")
    _, out = capsys.readouterr()

    assert "Table pytest_prod was deleted!" not in out
    assert not table.table_exists("staging")


def test_delete_prod():
    """
    Test delete method with prod mode
    """
    table.create(
        path=csv_path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="replace",
        source_format="csv",
    )

    table.delete("prod")

    assert not table.table_exists("prod")
    assert table.table_exists("staging")
