from basedosdados.upload.table import Table
from basedosdados.upload.dataset import Dataset
from basedosdados.exceptions import BaseDosDadosException
import pytest

DATASET_ID = "pytest"
TABLE_ID = "pytest"
csvPath = "tests/test_upload/table/municipio.csv"
avroPath = "tests/test_upload/table/municipio.avro"
parquetPath = "tests/test_upload/table/municipio.parquet"
sqlPath = "tests/test_upload/table/publish.sql"

table = Table(dataset_id=DATASET_ID, table_id=TABLE_ID)


# def test_get_biglake_connection(): improve the understanding of the connection location & mode, and the permissions required
#     """
#     Test what is the connection and if connects with biglake
#     """
# 
#     out = table._get_biglake_connection(mode='staging')
#     print('connection: ', out)
#     assert isinstance(out, str)

def test_get_table_description():
    """
    Test if get the correct description of a table
    """

    out = table._get_table_description('staging')

    assert isinstance(out, str)
    assert 'staging table for `basedosdados-dev.pytest.pytest' in out


def test_create():
    """
    Test create method
    """

    table.create(
        path=csvPath,
        if_table_exists="pass",
        if_dataset_exists="pass",
        if_storage_data_exists="pass",
    )

    out = table.table_exists("staging")

    assert out is True


def test_append(capsys):
    """
    Test append method
    """

    table.append(csvPath)
    _, out = capsys.readouterr()

    assert "Table pytest was appended!" in out


def test_create_table_exists():
    """
    Test create if table already exists
    """

    with pytest.raises(FileExistsError):
        table.create(
            path=csvPath,
            if_table_exists="raise",
            if_dataset_exists="pass",
            if_storage_data_exists="pass",
        )


def test_get_columns_from_bq():
    """
    Test if get the columns
    """

    out = table._get_columns_from_bq('staging')

    assert isinstance(out, dict)
    assert len(out) > 0
    assert len(out) != 0


def test_get_columns_from_data():
    """
    Test if get the columns from data
    """
    out = table._get_columns_from_data(csvPath, mode='staging')

    assert isinstance(out, dict)
    assert 10 > len(out['columns']) > 0 
    assert len(out) > 0
    assert len(out) != 0


def test_create_dataset_and_storage_exists():
    """
    Test create if dataset and storage already exists
    """

    with pytest.raises(BaseDosDadosException):
        table.create(
            path=csvPath,
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
        path=parquetPath,
        source_format="parquet",
        if_table_exists="replace",
        if_storage_data_exists="pass",
        if_dataset_exists="replace",
    )
    assert table.table_exists("staging")


def test_table_create_with_avro_source_format(capsys):
    """
    Test create when source format is avro
    """

    table.create(
        path=avroPath,
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
            path=csvPath,
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
        path=csvPath,
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
        path=csvPath,
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
        path=csvPath,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_dataset_exists="replace",
        source_format="csv",
    )

    table.delete("prod")

    assert not table.table_exists("prod")
    assert table.table_exists("staging")
