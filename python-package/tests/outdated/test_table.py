"""
Tests for the Table class
"""
import shutil


from pathlib import Path

import pytest
from google.api_core.exceptions import NotFound

import basedosdados as bd
from basedosdados import Dataset, Storage, Table
from basedosdados.exceptions import BaseDosDadosException

DATASET_ID = "pytest"
TABLE_ID = "pytest"

TABLE_FILES = ["publish.sql", "table_config.yaml"]


def _check_config_files(folder):
    """
    Private method to check that the config files are created
    """

    for file in TABLE_FILES:
        assert (folder / file).exists()


def test_init(
    table,
    testdir,
    folder,
    data_csv_path,
):
    """
    Test the init method
    """

    # remove folder
    shutil.rmtree(testdir / DATASET_ID / TABLE_ID, ignore_errors=True)

    Dataset(dataset_id=DATASET_ID, metadata_path=testdir).init(replace=True)

    table.init(data_sample_path=data_csv_path)

    _check_config_files(folder)


def test_init_file_exists_error(table, data_csv_path):
    """
    Test if error is raised when the table config file exists
    """

    with pytest.raises(FileExistsError):
        table.init(if_folder_exists="raise", if_table_config_exists="replace")

    with pytest.raises(FileExistsError):
        table.init(
            data_sample_path=data_csv_path,
            if_folder_exists="replace",
            if_table_config_exists="raise",
        )


def test_init_not_implemented_error(table, data_csv_path):
    """
    Test if error is thrown when data format is not implemented
    """

    with pytest.raises(NotImplementedError):
        table.init(
            if_folder_exists="replace",
            if_table_config_exists="replace",
            data_sample_path=data_csv_path,
            source_format="json",
        )


def test_init_no_path(table):
    """
    Test if error is thrown when no path is provided
    """

    with pytest.raises(BaseDosDadosException):
        table.init(if_folder_exists="replace", if_table_config_exists="replace")

    with pytest.raises(BaseDosDadosException):
        table.init(
            if_folder_exists="replace",
            if_table_config_exists="raise",
        )


def test_init_if_folder_exists_pass_if_table_config_replace(table, folder):
    """
    Test if table config is replaced when if_folder_exists is "pass"
    """

    table.init(if_folder_exists="pass", if_table_config_exists="replace")

    _check_config_files(folder)


def test_init_if_folder_exists_replace_if_table_config_raise(
    table, folder, data_csv_path
):
    """
    Test if config files are created when if_folder_exists is "replace"
    """
    shutil.rmtree(table.table_folder)

    table.init(
        data_sample_path=data_csv_path,
        if_folder_exists="replace",
        if_table_config_exists="raise",
    )
    _check_config_files(folder)


def test_init_if_folder_exists_replace_if_table_config_exists_replace(
    table, folder, data_csv_path
):
    """
    Test if config files are created when if_folder_exists is "replace" and if_table_config_exists is "replace"
    """

    table.init(
        data_sample_path=data_csv_path,
        if_folder_exists="replace",
        if_table_config_exists="replace",
    )

    _check_config_files(folder)


def table_exists(table, mode):
    """
    Test if table exists in BigQuery
    """

    try:
        table.client[f"bigquery_{mode}"].get_table(table.table_full_name[mode])
        return True
    except NotFound:
        return False


def test_delete_all(table, data_csv_path):
    """
    Teste delete method with all modes
    """
    table.create(
        path=data_csv_path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists="replace",
        source_format="csv",
    )

    table.delete(mode="all")

    assert not table_exists(table, mode="staging")
    assert not table_exists(table, mode="prod")


def test_delete_prod(table, data_csv_path):
    """
    Test delete method with prod mode
    """
    table.create(
        path=data_csv_path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists="replace",
        source_format="csv",
    )

    table.delete(mode="prod")

    assert not table_exists(table, mode="prod")
    assert table_exists(table, mode="staging")


def test_delete_staging(table, data_csv_path):
    """
    Test delete method with staging mode
    """
    table.create(
        path=data_csv_path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists="replace",
        source_format="csv",
    )

    table.delete(mode="staging")

    assert not table_exists(table, mode="staging")


def test_create_no_path_error(table, testdir):
    """
    Teste if error is raised when no path is provided
    """

    shutil.rmtree(testdir / DATASET_ID / TABLE_ID, ignore_errors=True)

    Dataset(dataset_id=DATASET_ID, metadata_path=testdir).create(if_exists="pass")

    with pytest.raises(BaseDosDadosException):
        table.create(if_storage_data_exists="replace")

    with pytest.raises(BaseDosDadosException):
        table.create(if_table_config_exists="replace")


def test_create_no_path(table, testdir, data_csv_path, sample_data):
    """
    Test create when path is provided in init
    """

    Storage(dataset_id=DATASET_ID, table_id=TABLE_ID, metadata_path=testdir).upload(
        data_csv_path, mode="staging", if_exists="replace"
    )

    table.init(data_sample_path=data_csv_path, if_folder_exists="replace")

    for file in TABLE_FILES:
        shutil.copy(sample_data / file, table.table_folder / file)

    table.create(if_storage_data_exists="pass", if_table_config_exists="pass")
    assert table_exists(table, "staging")


def test_create_storage_data_exist_table_config_exist(
    table, testdir, data_csv_path, sample_data
):
    """
    Test create when storage data exists and table config exists
    """

    shutil.rmtree(testdir / DATASET_ID / TABLE_ID, ignore_errors=True)

    Dataset(dataset_id=DATASET_ID, metadata_path=testdir).create(if_exists="pass")

    Storage(dataset_id=DATASET_ID, table_id=TABLE_ID, metadata_path=testdir).upload(
        data_csv_path, mode="staging", if_exists="replace"
    )

    table.init(
        data_sample_path=data_csv_path,
        if_folder_exists="replace",
        if_table_config_exists="replace",
    )

    for file in TABLE_FILES:
        shutil.copy(sample_data / file, table.table_folder / file)

    table.delete(mode="all")

    table.create(
        data_csv_path,
        if_storage_data_exists="pass",
        if_table_config_exists="pass",
    )
    assert table_exists(table, "staging")


def test_create_if_table_exist_replace(table):
    """
    Test create when if_table_exist is "replace"
    """

    table.create(
        if_table_exists="replace",
        if_storage_data_exists="pass",
        if_table_config_exists="pass",
    )
    assert table_exists(table, "staging")


def test_table_create_parquet_implemented_source_format(data_parquet_path):
    """
    Test create when source format is parquet
    """
    table = bd.Table("ds_test", "tb_test")
    table.create(
        path=data_parquet_path,
        if_table_exists="replace",
        if_storage_data_exists="pass",
        if_table_config_exists="replace",
        source_format="parquet",
    )
    assert table_exists(table, "staging")


def test_table_create_csv_implemented_source_format(data_csv_path):
    """
    Test create when source format is csv
    """
    table = bd.Table(dataset_id="ds_test", table_id="tb_test")
    table.create(
        path=data_csv_path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists="replace",
        source_format="csv",
    )
    assert table_exists(table, "staging")


@pytest.mark.skip(reason="External configuration not fully implemented")
def test_table_create_avro_implemented_source_format(data_avro_path):
    """
    Test create when source format is avro
    """
    table = bd.Table("ds_test", "tb_test")
    table.create(
        path=data_avro_path,
        if_table_exists="replace",
        if_storage_data_exists="pass",
        if_table_config_exists="replace",
        source_format="avro",
    )
    assert table_exists(table, "staging")


def test_table_create_not_implemented_source_format(table, data_csv_path):
    """
    Test create when source format is not implemented
    """

    with pytest.raises(NotImplementedError):
        table.create(
            path=data_csv_path,
            if_table_exists="replace",
            if_storage_data_exists="pass",
            if_table_config_exists="pass",
            source_format="json",
        )


def test_create_if_table_exists_pass(table):
    """
    Test create when if_table_exist is "pass"
    """
    table.create(
        if_table_exists="pass",
        if_storage_data_exists="pass",
        if_table_config_exists="pass",
    )
    assert table_exists(table, "staging")


def test_create_if_storage_data_replace_if_table_config_pass(table, data_csv_path):
    """
    Test create when if_storage_data_exists is "replace" and if_table_config_exists is "pass"
    """
    table.delete("all")

    table.create(
        data_csv_path,
        if_storage_data_exists="replace",
        if_table_config_exists="pass",
    )
    assert table_exists(table, "staging")


def test_create_if_table_exists_raise(table):
    """
    Test create when if_table_exist is "raise"
    """

    with pytest.raises(FileExistsError):
        table.create(if_storage_data_exists="pass", if_table_config_exists="pass")


def test_create_with_path(table, testdir, data_csv_path):
    """
    Test create with path only
    """

    table.delete("all")
    Storage(DATASET_ID, TABLE_ID).delete_table(not_found_ok=True)
    shutil.rmtree(testdir / DATASET_ID / TABLE_ID, ignore_errors=True)

    table.create(
        data_csv_path,
    )
    assert table_exists(table, mode="staging")


def test_create_if_storage_data_pass_if_table_config_pass(table, sample_data):
    """
    Test create when if_storage_data_exists is "pass" and if_table_config_exists is "pass"
    """
    for file in TABLE_FILES:
        shutil.copy(sample_data / file, table.table_folder / file)

    table.delete("all")

    table.create(
        if_storage_data_exists="pass",
        if_table_config_exists="pass",
    )
    assert table_exists(table, mode="staging")


def test_create_if_storage_data_pass_if_table_config_replace(table, data_csv_path):
    """
    Test create when if_storage_data_exists is "pass" and if_table_config_exists is "replace"
    """

    table.delete("all")

    table.create(
        data_csv_path, if_storage_data_exists="pass", if_table_config_exists="replace"
    )
    assert table_exists(table, mode="staging")


def test_create_if_folder_exists_raise(table, data_csv_path):
    """
    Test create when if_folder_exists is "raise"
    """
    with pytest.raises(FileExistsError):
        table.create(
            data_csv_path, if_table_exists="pass", if_storage_data_exists="pass"
        )


def test_create_with_upload(table, data_csv_path):
    """
    Test create with upload
    """

    table.delete("all")

    Storage(DATASET_ID, TABLE_ID).delete_table(not_found_ok=True)

    table.create(data_csv_path, if_table_config_exists="replace")
    assert table_exists(table, mode="staging")


def test_create_if_storage_data_replace_if_table_config_replace(table, data_csv_path):
    """
    Test create when if_storage_data_exists is "replace" and if_table_config_exists is "replace"
    """
    table.delete("all")
    table.create(
        data_csv_path,
        if_storage_data_exists="replace",
        if_table_config_exists="replace",
    )
    assert table_exists(table, mode="staging")


def test_create_if_storage_data_raise(table, testdir, data_csv_path):
    """
    Test create when if_storage_data_exists is "raise"
    """

    Storage(dataset_id=DATASET_ID, table_id=TABLE_ID, metadata_path=testdir).upload(
        data_csv_path, mode="staging", if_exists="replace"
    )

    with pytest.raises(Exception):
        table.create(
            data_csv_path,
            if_table_exists="replace",
            if_table_config_exists="replace",
            if_storage_data_exists="raise",
        )


def test_create_if_force_columns_true(testdir):
    """
    Test create when if_force_columns is True
    """
    dataset_id = "br_cvm_administradores_carteira"
    table_id = "pessoa_fisica"

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id, metadata_path=testdir)

    client = Storage(dataset_id=dataset_id, table_id=table_id)
    client.download("bd_pessoa_fisica.csv", testdir, mode="staging")

    filepath = (
        testdir
        / "staging"  # noqa
        / "br_cvm_administradores_carteira"  # noqa
        / "pessoa_fisica"  # noqa
        / "bd_pessoa_fisica.csv"  # noqa
    )

    tb.create(
        filepath,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists="replace",
        force_columns=True,
    )

    assert tb.table_config["columns"][0]["description"] is None


def test_create_if_force_columns_false(testdir):
    """
    Test create when if_force_columns is True
    """
    dataset_id = "br_cvm_administradores_carteira"
    table_id = "pessoa_fisica"

    tb = bd.Table(dataset_id=dataset_id, table_id=table_id, metadata_path=testdir)

    client = Storage(dataset_id=dataset_id, table_id=table_id)
    client.download("bd_pessoa_fisica.csv", testdir, mode="staging")

    filepath = (
        testdir
        / "staging"  # noqa
        / "br_cvm_administradores_carteira"  # noqa
        / "pessoa_fisica"  # noqa
        / "bd_pessoa_fisica.csv"  # noqa
    )

    tb.create(
        filepath,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists="replace",
        force_columns=False,
    )

    assert tb.table_config["columns"][0]["description"] == "Nome"


def test_create_auto_partitions(testdir, data_csv_path, sample_data):
    """
    Test create with auto_partitions
    """
    shutil.rmtree(testdir / "partitions", ignore_errors=True)

    table_part = Table(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID + "_partitioned",
        metadata_path=testdir,
    )

    Path(testdir / "partitions").mkdir()

    shutil.copy(
        sample_data / "table_config_part.yaml",
        Path(testdir / "partitions" / "table_config.yaml"),
    )
    shutil.copy(
        sample_data / "publish_part.sql",
        testdir / "partitions" / "publish.sql",
    )
    for n in [1, 2]:
        Path(testdir / "partitions" / f"keys={n}").mkdir()
        shutil.copy(
            data_csv_path,
            testdir / "partitions" / f"keys={n}" / "municipio.csv",
        )

    table_part.create(
        path=testdir / "partitions",
        if_table_exists="replace",
        if_table_config_exists="replace",
        if_storage_data_exists="replace",
    )
    assert table_exists(table_part, "staging")

    table_part.publish(if_exists="replace")

    assert table_exists(table_part, "prod")


def test_update_raises(testdir, sample_data, capsys):
    """
    Test update raises
    """

    table_part = Table(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID + "_partitioned",
        metadata_path=testdir,
    )

    shutil.copy(
        sample_data / "table_config_part_wrong.yaml",
        testdir / DATASET_ID / "pytest_partitioned" / "table_config.yaml",
    )

    with pytest.raises(Exception):
        table_part.update("all")
        out, _ = capsys.readouterr()
        assert "publish.sql" in out

    shutil.copy(
        sample_data / "publish_part.sql",
        table_part.table_folder / "publish.sql",
    )
    shutil.copy(
        sample_data / "table_config.yaml",
        Path(table_part.table_folder / "table_config.yaml"),
    )

    with pytest.raises(Exception):
        table_part.update("all")
        assert "table_config.yaml" in out


def test_update(table, data_csv_path):
    """
    Test update method
    """
    table.create(
        data_csv_path,
        if_table_exists="pass",
        if_storage_data_exists="pass",
        if_table_config_exists="pass",
    )

    assert table_exists(table, "staging")

    # Como dar assert que a descrição foi atualizada?

    table.update(mode="all")


def test_publish(table, testdir, sample_data, data_csv_path):
    """
    Test publish method
    """
    table.delete("all")

    shutil.copy(
        sample_data / "publish.sql",
        testdir / DATASET_ID / TABLE_ID / "publish.sql",
    )

    shutil.copy(
        sample_data / "table_config.yaml",
        testdir / DATASET_ID / TABLE_ID / "table_config.yaml",
    )

    table.create(
        data_csv_path,
        if_table_exists="replace",
        if_storage_data_exists="replace",
        if_table_config_exists="pass",
    )

    table.publish(if_exists="replace")

    assert table_exists(table, "prod")


def test_append(table, testdir, data_csv_path):
    """
    Test append method
    """
    shutil.copy(
        data_csv_path,
        testdir / "municipio2.csv",
    )

    table.append((testdir / "municipio2.csv"))
