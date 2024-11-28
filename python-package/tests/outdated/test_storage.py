"""
Tests for the Storage class
"""

import os
from pathlib import Path

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import storage as storage_gcloud

import basedosdados as bd
from basedosdados import Storage

DATASET_ID = "pytest"
TABLE_ID = "pytest"
SAVEPATH = Path(__file__).parent / "tmp_bases"
TABLE_FILES = ["publish.sql", "table_config.yaml"]


def test_upload(storage, sample_data):
    """
    Test the upload method
    """

    storage.delete_file("municipio.csv", "staging", not_found_ok=True)

    storage.upload(sample_data / "municipio.csv", mode="staging")

    with pytest.raises(Exception):
        storage.upload(sample_data / "municipio.csv", mode="staging")

    storage.upload(sample_data / "municipio.csv", mode="staging", if_exists="replace")

    storage.upload(
        sample_data / "municipio.csv",
        mode="staging",
        if_exists="replace",
        partitions="key1=value1/key2=value2",
    )

    storage.upload(
        sample_data / "municipio.csv",
        mode="staging",
        if_exists="replace",
        partitions={"key1": "value1", "key2": "value1"},
    )

    with pytest.raises(Exception):
        storage.upload(
            sample_data / "municipio.csv",
            mode="staging",
            if_exists="replace",
            partitions=["key1", "value1", "key2", "value1"],
        )


def test_download_not_found(storage):
    """
    Test the download method when the file does not exist
    """

    with pytest.raises(FileNotFoundError):
        storage.download(filename="not_found", savepath=SAVEPATH)

    with pytest.raises(FileNotFoundError):
        storage.download(savepath=SAVEPATH)


def test_download_filename(storage):
    """
    Test the download method with a filename
    """

    storage.download(filename="municipio.csv", savepath=SAVEPATH, mode="staging")

    assert (
        Path(SAVEPATH) / "staging" / DATASET_ID / TABLE_ID / "municipio.csv"
    ).is_file()


def test_download_partitions(storage):
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
        / "staging"  # noqa
        / DATASET_ID  # noqa
        / TABLE_ID  # noqa
        / "key1=value1"  # noqa
        / "key2=value1"  # noqa
        / "municipio.csv"  # noqa
    ).is_file()

    storage.download(
        savepath=SAVEPATH,
        mode="staging",
        partitions={"key1": "value1", "key2": "value2"},
    )

    assert (
        Path(SAVEPATH)
        / "staging"  # noqa
        / DATASET_ID  # noqa
        / TABLE_ID  # noqa
        / "key1=value1"  # noqa
        / "key2=value2"  # noqa
        / "municipio.csv"  # noqa
    ).is_file()


def test_download_default(storage):
    """
    Test the download method with the default mode
    """
    storage.download(savepath=SAVEPATH, mode="staging")

    assert (
        Path(SAVEPATH) / "staging" / DATASET_ID / TABLE_ID / "municipio.csv"
    ).is_file()


def test_delete_file(storage):
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

    Storage("br_ibge_pib", "municipio").copy_table()

    with pytest.raises(FileNotFoundError):
        Storage("br_ibge_pib2", "municipio2").copy_table()

    Storage("br_ibge_pib", "municipio").copy_table(
        destination_bucket_name="basedosdados-dev",
    )


def test_delete_table():
    """
    Test the delete_table method
    """

    Storage("br_ibge_pib", "municipio").delete_table(bucket_name="basedosdados-dev")

    with pytest.raises(FileNotFoundError):
        Storage("br_ibge_pib", "municipio").delete_table()


def test_change_path_credentials(storage, sample_data):
    """
    Test the change_path_credentials method
    """

    storage.delete_file("municipio.csv", "staging", not_found_ok=True)

    storage.upload(sample_data / "municipio.csv", mode="staging")

    # check if .basedosdados folder exists
    if not Path.home().joinpath(".basedosdados").exists():
        raise FileNotFoundError("You need to run the init command first")

    # get home dir
    home = str(Path.home())

    os.system(f"mkdir {home}/.testcredentials")
    os.system(f"mv -r {home}/.basedosdados/* .testcredentials")
    os.system(
        "sed -i 's/\/.basedosdados\//\/.testcredentials\//g' config.toml"  # noqa
    )

    bd.config.project_config_path = f"{home}/.testcredentials"

    storage.copy_table(
        source_bucket_name="basedosdados-dev",
        destination_bucket_name="basedosdados-dev-backup",
    )

    # check if file exist in new bucket
    client = storage_gcloud.Client()
    files = [blob.name for blob in client.list_blobs("basedosdados-dev-backup")]

    # delete file from new bucket
    file = f"staging/{DATASET_ID}/{TABLE_ID}/municipio.csv"
    bucket = client.get_bucket("basedosdados-dev-backup")
    blob = bucket.blob(file)
    blob.delete()

    # move again .basedosdados folder
    os.system(f"mv -r {home}/.testcredentials/* .basedosdados")
    # replace path in config.toml
    os.system(
        "sed -i 's/\/.testcredentials\//\/.basedosdados\//g' config.toml"  # noqa
    )

    os.system(f"rm -r {home}/.testcredentials")

    assert file in files
