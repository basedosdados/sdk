from basedosdados.upload.dataset import Dataset

DATASET_ID = "pytest"


def test_create():
    """
    Test the create function
    """
    dataset = Dataset(dataset_id=DATASET_ID)

    dataset.create(if_exists="raise")

    dataset.delete()
    assert not dataset.exists()

    dataset.create(if_exists="replace")

    dataset.create(if_exists="update")

    dataset.create(if_exists="pass")
    assert dataset.exists()


def test_exists():
    """
    Test the exists function
    """
    dataset = Dataset(dataset_id=DATASET_ID)

    out = dataset.exists()
    assert isinstance(out, bool)
    assert out is True


def test_delete():
    """
    Test the delete function
    """
    dataset = Dataset(dataset_id=DATASET_ID)

    dataset.delete()

    out = dataset.exists()
    assert isinstance(out, bool)
    assert out is False


def test_update():
    """
    Test the update function
    """
    dataset = Dataset(dataset_id=DATASET_ID)

    dataset.create(if_exists="pass")
    assert dataset.exists()


def test_loop_modes():
    """
    Test the loop_modes function
    """
    dataset = Dataset(dataset_id="")

    assert len(list(dataset._loop_modes(mode="all"))) == 2
    assert len(list(dataset._loop_modes(mode="staging"))) == 1
    assert "staging" in next(dataset._loop_modes(mode="staging"))["id"]
    assert len(list(dataset._loop_modes(mode="prod"))) == 1


def test_publicize():
    """
    Test the publicize function
    """
    dataset = Dataset(dataset_id=DATASET_ID)

    dataset.create(if_exists="pass")
    dataset.publicize()
