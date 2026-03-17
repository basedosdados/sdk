import pytest

from basedosdados.upload.dataset import Dataset
from tests.constants import DATASET_ID_PREFIX

dataset_id = f"{DATASET_ID_PREFIX}_test_dataset"


@pytest.mark.order1
def test_create():
    """
    Test the create function
    """

    dataset = Dataset(dataset_id=dataset_id)

    if dataset.exists():
        dataset.delete()
        assert not dataset.exists()

    dataset.create(if_exists="raise")

    dataset.create(if_exists="replace")

    dataset.create(if_exists="update")

    dataset.create(if_exists="pass")
    assert dataset.exists()


@pytest.mark.order2
def test_exists():
    """
    Test the exists function
    """
    dataset = Dataset(dataset_id=dataset_id)

    out = dataset.exists()
    assert isinstance(out, bool)
    assert out is True


@pytest.mark.order3
def test_update():
    """
    Test the update function
    """
    dataset = Dataset(dataset_id=dataset_id)

    dataset.create(if_exists="pass")
    assert dataset.exists()


@pytest.mark.order4
def test_loop_modes():
    """
    Test the loop_modes function
    """
    dataset = Dataset(dataset_id=dataset_id)

    assert len(list(dataset._loop_modes(project_gcp="staging"))) == 1
    assert "staging" in next(dataset._loop_modes(project_gcp="staging"))["id"]
    assert len(list(dataset._loop_modes(project_gcp="prod"))) == 1


@pytest.mark.order6
def test_delete():
    """
    Test the delete function
    """
    dataset = Dataset(dataset_id=dataset_id)

    dataset.delete()

    out = dataset.exists()
    assert isinstance(out, bool)
    assert out is False
