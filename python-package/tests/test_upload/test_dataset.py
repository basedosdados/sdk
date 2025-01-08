import pytest
import google.api_core.exceptions as google_exceptions
from basedosdados.upload.dataset import Dataset

def test_exists():

  dataset = Dataset(dataset_id='1')
  
  out = dataset.exists()

  assert isinstance(out, bool)

def test_create():
    """
    Test the create function
    """

    dataset = Dataset(dataset_id='1')

    dataset.create(if_exists='raise')
    dataset.delete()
    assert not dataset.exists()

    dataset.create(if_exists='replace')

    dataset.create(if_exists='update')

    dataset.create(if_exists='pass')

    assert dataset.exists()


def test_delete():

  dataset = Dataset(dataset_id='1')

  dataset.delete()

  out = dataset.exists()

  assert isinstance(out, bool)
  assert not out

def test_update():
    """
    Test the update function
    """
    dataset = Dataset(dataset_id='1')

    dataset.create(if_exists="pass")

    assert dataset.exists()

def test_loop_modes():
    """
    Test the loop_modes function
    """

    dataset = Dataset(dataset_id='')
    assert len(list(dataset._loop_modes(mode="all"))) == 2
    assert len(list(dataset._loop_modes(mode="staging"))) == 1
    assert "staging" in next(dataset._loop_modes(mode="staging"))["id"]
    assert len(list(dataset._loop_modes(mode="prod"))) == 1

def test_publicize():
    """
    Test the publicize function
    """
    dataset = Dataset(dataset_id='1')

    dataset.create(if_exists='pass')

    dataset.publicize()