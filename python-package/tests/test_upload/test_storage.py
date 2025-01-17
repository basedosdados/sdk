import pytest
from basedosdados.upload.storage import Storage
from google.api_core.exceptions import NotFound
from pathlib import Path

csv_path = "tests/test_upload/table/municipio.csv"
SAVEPATH = Path(__file__).parent / "tmp_bases"
DATASET_ID = "pytest"
TABLE_ID = "pytest"

storage = Storage(dataset_id=DATASET_ID, table_id=TABLE_ID)


def test_upload_with_errors():
    """
    Test the upload method raise errors
    """
    storage.delete_file(csv_path, mode="staging", not_found_ok=True)
    storage.upload(csv_path, mode="staging", if_exists="pass")

    with pytest.raises(Exception):
        storage.upload(csv_path, mode="staging")
    storage.upload(csv_path, mode="staging", if_exists="replace")
    storage.upload(
        csv_path,
        mode="staging",
        if_exists="replace",
        partitions="key1=value1/key2=value2",
    )
    storage.upload(
        csv_path,
        mode="staging",
        if_exists="replace",
        partitions={"key1": "value1", "key2": "value1"},
    )
    with pytest.raises(Exception):
        storage.upload(
            csv_path,
            mode="staging",
            if_exists="replace",
            partitions=["key1", "value1", "key2", "value1"],
        )


def test_download_not_found():
    """
    Test the download method when the file does not exist
    """

    with pytest.raises(FileNotFoundError):
        storage.download(filename="not_found", savepath=SAVEPATH)


def test_download_filename():
    """
    Test the download method with a filename
    """

    storage.download(
        filename="municipio.csv", savepath=SAVEPATH, mode="staging"
    )
    assert (
        Path(SAVEPATH) / "staging" / DATASET_ID / TABLE_ID / "municipio.csv"
    ).is_file()


def test_download_partitions():
    """
    Test the download method with partitions
    """

    storage.download(
        savepath=SAVEPATH,
        mode="staging",
        partitions="key1=value1/key2=value1/",
    )

    assert (
        Path(SAVEPATH)
        / "staging"
        / DATASET_ID
        / TABLE_ID
        / "key1=value1"
        / "key2=value1"
        / "municipio.csv"
    ).is_file()

    storage.download(
        savepath=SAVEPATH,
        mode="staging",
        partitions={"key1": "value1", "key2": "value2"},
    )

    assert (
        Path(SAVEPATH)
        / "staging"
        / DATASET_ID
        / TABLE_ID
        / "key1=value1"
        / "key2=value2"
        / "municipio.csv"
    ).is_file()


def test_download_default():
    """
    Test the download method with the default mode
    """

    storage.download(savepath=SAVEPATH, mode="staging")

    assert (
        Path(SAVEPATH) / "staging" / DATASET_ID / TABLE_ID / "municipio.csv"
    ).is_file()


def test_delete_file():
    """
    Test the delete_file method
    """

    storage.delete_file("municipio.csv", "staging")

    with pytest.raises(NotFound):
        storage.delete_file("municipio.csv", "staging")
        storage.delete_file("municipio.csv", "staging", not_found_ok=True)


def test_copy_table():
    """
    Test the copy_table method
    """

    storage.copy_table()

    with pytest.raises(FileNotFoundError):
        Storage("br_ibge_pib2", "municipio2").copy_table()

    storage.copy_table(
        destination_bucket_name="basedosdados-dev",
    )


def test_delete_table():
    """
    Test the delete_table method
    """

    storage.delete_table(bucket_name="basedosdados-dev")

    with pytest.raises(FileNotFoundError):
        storage.delete_table()
