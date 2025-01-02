"""
Tests for the Dataset class
"""
import shutil


from pathlib import Path

import google.api_core.exceptions as google_exceptions
import pytest
from google.cloud import bigquery

DATASET_ID = "pytest"

DATASET_FILES = ["code", "dataset_config.yaml", "README.md"]


def _check_files(testdir):
    """
    Check if the files are in the folder
    """
    for file in DATASET_FILES:
        assert (testdir / file).exists()


def _dataset_exists(dataset):
    """
    Check if the dataset exists
    """
    try:
        bq_datasets = [
            m["client"].get_dataset(m["id"]) for m in dataset._loop_modes("all")
        ]
        return isinstance(bq_datasets[0], bigquery.Dataset)
    except google_exceptions.NotFound:
        return False


def test_init(dataset, testdir):
    """
    Test the init function
    """
    # remove folder
    shutil.rmtree(Path(testdir))

    folder = Path(testdir) / DATASET_ID

    dataset.init()
    assert folder.exists()

    _check_files(folder)

    dataset.init(replace=True)
    assert folder.exists()

    _check_files(folder)


def test_delete(dataset):
    """
    Test the delete function
    """
    dataset.delete(mode="all")

    assert not _dataset_exists(dataset)


def test_create(dataset):
    """
    Test the create function
    """
    dataset.delete(mode="all")

    dataset.create()

    assert _dataset_exists(dataset)

    with pytest.raises(google_exceptions.Conflict):
        dataset.create(if_exists="raise")

    dataset.create(if_exists="replace")

    dataset.create(if_exists="update")

    dataset.create(if_exists="pass")

    assert _dataset_exists(dataset)


def test_update(dataset):
    """
    Test the update function
    """
    dataset.create(if_exists="pass")

    assert _dataset_exists(dataset)

    dataset.update()


def test_publicize(dataset):
    """
    Test the publicize function
    """
    dataset.create(if_exists="pass")

    dataset.publicize()


def test_loop_modes(dataset):
    """
    Test the loop_modes function
    """
    assert len(list(dataset._loop_modes(mode="all"))) == 2
    assert len(list(dataset._loop_modes(mode="staging"))) == 1
    assert "staging" in next(dataset._loop_modes(mode="staging"))["id"]
    assert len(list(dataset._loop_modes(mode="prod"))) == 1
