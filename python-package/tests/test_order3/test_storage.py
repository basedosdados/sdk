from pathlib import Path

import pytest
from google.api_core.exceptions import NotFound

from basedosdados.upload.storage import Storage
from tests.constants import DATASET_ID_PREFIX, TABLE_ID_PREFIX

csv_path = "tests/test_upload/table/municipio.csv"
SAVEPATH = Path(__file__).parent / "tmp_bases"

dataset_id = f"{DATASET_ID_PREFIX}_test_storage"
table_id = f"{TABLE_ID_PREFIX}_test_storage"

storage = Storage(dataset_id=dataset_id, table_id=table_id)


@pytest.mark.order1
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


@pytest.mark.order2
def test_download_filename():
    """
    Test the download method with a filename
    """

    storage.download(
        filename="municipio.csv", savepath=SAVEPATH, mode="staging"
    )
    assert (
        Path(SAVEPATH) / "staging" / dataset_id / table_id / "municipio.csv"
    ).is_file()


@pytest.mark.order2
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
        / dataset_id
        / table_id
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
        / dataset_id
        / table_id
        / "key1=value1"
        / "key2=value2"
        / "municipio.csv"
    ).is_file()


@pytest.mark.order2
def test_download_default():
    """
    Test the download method with the default mode
    """

    storage.download(savepath=SAVEPATH, mode="staging")

    assert (
        Path(SAVEPATH) / "staging" / dataset_id / table_id / "municipio.csv"
    ).is_file()


@pytest.mark.order2
def test_copy_table():
    """
    Test the copy_table method
    """

    new_table_id = f"{table_id}_copy_table"

    # Create copy from folder on storage
    storage.copy_table(
        source_bucket_name="basedosdados-dev",
        destination_bucket_name="basedosdados-dev",
        new_table_id=new_table_id,
    )

    savepath = SAVEPATH / "storage_test_copy_table"

    # Download file copied
    st = Storage(dataset_id=new_table_id, table_id=new_table_id)

    st.download(filename="municipio.csv", savepath=savepath)

    assert (
        savepath
        / "staging"
        / new_table_id  # dataset_id
        / new_table_id
        / "municipio.csv"
    ).exists()

    # Cleanup
    st.delete_table()


@pytest.mark.order3
def test_delete_file():
    """
    Test the delete_file method
    """

    storage.delete_file("municipio.csv", "staging")

    # Ensures the file has been deleted
    with pytest.raises(NotFound):
        storage.delete_file("municipio.csv", "staging")
        storage.delete_file("municipio.csv", "staging", not_found_ok=True)


@pytest.mark.order3
def test_delete_table():
    """
    Test the delete_table method
    """

    storage.delete_table(bucket_name="basedosdados-dev")

    with pytest.raises(FileNotFoundError):
        storage.delete_table()
