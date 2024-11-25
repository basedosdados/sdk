"""
Share fixtures for tests.
"""

import shutil
import sys


from pathlib import Path

import pytest
import ruamel.yaml as ryaml

from basedosdados import Metadata  # TODO: deprecate
from basedosdados import Dataset, Storage, Table
from basedosdados.core.base import Base

DATASET_ID = "pytest"
TABLE_ID = "pytest"

METADATA_FILES = {"dataset": "dataset_config.yaml", "table": "table_config.yaml"}


@pytest.fixture(name="testdir")
def fixture_testdir():
    """
    Fixture that returns a temporary directory for the metadata files.
    """
    try:
        (Path(__file__).parent / "tmp_bases").mkdir(exist_ok=False)
    except Exception:
        pass
    return Path(__file__).parent / "tmp_bases"


@pytest.fixture(name="sample_data")
def fixture_sample_data(testdir):
    """
    Fixture that returns a directory with sample data.
    """
    return testdir.parent / "sample_data" / "table"


@pytest.fixture(name="dataset")
def fixture_dataset(testdir):
    """
    Fixture for the dataset class
    """
    return Dataset(dataset_id=DATASET_ID, metadata_path=testdir)


@pytest.fixture(name="dataset_metadata")
def fixture_dataset_metadata(testdir):
    """
    Fixture that returns a `Metadata` object for the dataset.
    """
    return Metadata(dataset_id=DATASET_ID, metadata_path=testdir)


@pytest.fixture(name="table_metadata")
def fixture_table_metadata(testdir):
    """
    Fixture that returns a `Metadata` object for the table.
    """
    return Metadata(dataset_id=DATASET_ID, table_id=TABLE_ID, metadata_path=testdir)


@pytest.fixture(name="dataset_metadata_path")
def fixture_dataset_metadata_path(testdir):
    """
    Fixture that returns the path to the dataset metadata file.
    """
    return Path(testdir) / DATASET_ID


@pytest.fixture(name="table_metadata_path")
def fixture_table_metadata_path(testdir):
    """
    Fixture that returns the path to the table metadata file.
    """
    return Path(testdir) / DATASET_ID / TABLE_ID


@pytest.fixture(name="invalid_dataset_metadata")
def fixture_invalid_dataset_metadata(testdir):
    """
    Fixture that returns invalid dataset metadata.
    """
    invalid_dataset_metadata = Metadata(
        dataset_id="br_ibge_pib",
        metadata_path=testdir,
    )
    invalid_dataset_metadata.create(if_exists="replace")

    invalid_config = invalid_dataset_metadata.local_metadata
    invalid_config["title"] = {"this_title": "is_not_valid"}

    print(invalid_dataset_metadata.filepath)

    with open(invalid_dataset_metadata.filepath, "w", encoding="utf-8") as file:
        ryaml.dump(invalid_config, file)

    return invalid_dataset_metadata


@pytest.fixture(name="invalid_table_metadata")
def fixture_invalid_table_metadata(testdir):
    """
    Fixture that returns invalid table metadata.
    """
    invalid_dataset_metadata = Metadata(
        dataset_id="br_ibge_pib",
        table_id="municipio",
        metadata_path=testdir,
    )
    invalid_dataset_metadata.create(if_exists="replace")

    invalid_config = invalid_dataset_metadata.local_metadata
    invalid_config["table_id"] = None

    with open(invalid_dataset_metadata.filepath, "w", encoding="utf-8") as file:
        ryaml.dump(invalid_config, file)

    return invalid_dataset_metadata


@pytest.fixture(name="valid_metadata_dataset")
def fixture_valid_metadata_dataset(testdir):
    """
    Fixture that returns a valid dataset metadata.
    """
    dataset_metadata = Metadata(dataset_id="br_ibge_pib", metadata_path=testdir)
    dataset_metadata.create(if_exists="replace")
    dataset_metadata.CKAN_API_KEY = "valid-key"
    return dataset_metadata


@pytest.fixture(name="valid_metadata_table")
def fixture_valid_metadata_table(testdir, dataset_metadata):
    """
    Fixture that returns a valid table metadata.
    """
    table_metadata = Metadata(
        dataset_id="br_ibge_pib",
        table_id="municipio",
        metadata_path=testdir,
    )
    table_metadata.create(if_exists="replace")
    dataset_metadata.CKAN_API_KEY = "valid-key"
    return table_metadata


@pytest.fixture(name="existent_metadata")
def fixture_existent_metadata(testdir):
    """
    Fixture that returns an existent metadata.
    """
    table_metadata_obj = Metadata(
        dataset_id="br_me_rais",
        table_id="microdados_vinculos",
        metadata_path=testdir,
    )
    return table_metadata_obj


@pytest.fixture(name="existent_metadata_path")
def fixture_existent_metadata_path(testdir):
    """
    Fixture that returns the path to the existent metadata file.
    """
    return Path(testdir) / "br_me_rais" / "microdados_vinculos"


@pytest.fixture(name="out_of_date_metadata_obj")
def fixture_out_of_date_metadata_obj(testdir):
    """
    Fixture that returns an output of date metadata.
    """
    out_of_date_metadata = Metadata(dataset_id="br_me_caged", metadata_path=testdir)
    out_of_date_metadata.create(if_exists="replace")

    out_of_date_config = out_of_date_metadata.local_metadata
    out_of_date_config["metadata_modified"] = "old_date"
    with open(out_of_date_metadata.filepath, "w", encoding="utf-8") as file:
        ryaml.dump(out_of_date_config, file)

    return out_of_date_metadata


@pytest.fixture(name="updated_metadata_obj")
def fixture_updated_metadata_obj(testdir):
    """
    Fixture that returns an updated metadata.
    """
    updated_metadata = Metadata(dataset_id="br_me_caged", metadata_path=testdir)
    updated_metadata.create(if_exists="replace")

    updated_config = updated_metadata.local_metadata
    updated_config["metadata_modified"] = updated_metadata.ckan_metadata[
        "metadata_modified"
    ]
    with open(updated_metadata.filepath, "w", encoding="utf-8") as file:
        ryaml.dump(updated_config, file)

    return updated_metadata


@pytest.fixture(name="pytest_dataset")
def fixture_pytest_dataset(testdir):
    """
    Fixture that returns a test dataset metadata.
    """
    shutil.rmtree(testdir, ignore_errors=True)
    pytest_dataset = Metadata(dataset_id="pytest", metadata_path=testdir)
    pytest_dataset.create(if_exists="replace")

    # fill dataset metadata for it to be publishable
    pytest_dataset_metadata = pytest_dataset.local_metadata
    pytest_dataset_metadata["organization"] = "acaps"  # set valid organization

    # materialize metadata file
    with open(pytest_dataset.filepath, "w", encoding="utf-8") as file:
        ryaml.dump(pytest_dataset_metadata, file)

    return pytest_dataset


@pytest.fixture(name="pytest_table")
def fixture_pytest_table(testdir):
    """
    Fixture that returns a test table metadata.
    """
    shutil.rmtree(testdir, ignore_errors=True)
    pytest_table = Metadata(dataset_id="pytest", table_id="pytest")
    pytest_table.create(if_exists="replace")
    return pytest_table


@pytest.fixture(name="invalid_organization_dataset")
def fixture_invalid_organization_dataset(testdir):
    """
    Fixture that returns an invalid organization dataset metadata.
    """
    invalid_organization_dataset = Metadata(
        dataset_id="br_ibge_pib",
        metadata_path=testdir,
    )
    invalid_organization_dataset.create(if_exists="replace")

    invalid_config = invalid_organization_dataset.local_metadata
    invalid_config["organization"] = "not-a-valid-organization"

    with open(invalid_organization_dataset.filepath, "w", encoding="utf-8") as file:
        ryaml.dump(invalid_config, file)

    return invalid_organization_dataset


@pytest.fixture(name="storage")
def fixture_storage(testdir):
    """
    Fixture for storage object.
    """
    return Storage(dataset_id=DATASET_ID, table_id=TABLE_ID, metadata_path=testdir)


@pytest.fixture(name="base")
def fixture_base():
    """
    Fixture for the base class
    """
    config_path = Path.home() / ".basedosdados_teste"
    return Base(config_path=config_path)


@pytest.fixture(name="config_file_exists")
def fixture_config_file_exists(base):
    """
    Fixture for the config_file_exists value
    """
    config_file = base.config_path / "config.toml"
    return config_file.exists()


@pytest.fixture(name="data_csv_path")
def fixture_data_csv_path(sample_data):
    """
    Fixture for data_csv_path
    """
    return sample_data / "municipio.csv"


@pytest.fixture(name="data_parquet_path")
def fixture_data_parquet_path(sample_data):
    """
    Fixture for data_parquet_path
    """
    return sample_data / "municipio.parquet"


@pytest.fixture(name="data_avro_path")
def fixture_data_avro_path(sample_data):
    """
    Fixture for data_avro_path
    """
    return sample_data / "municipio.avro"


@pytest.fixture(name="table")
def fixture_table(testdir):
    """
    Fixture for table object.
    """
    t = Table(dataset_id=DATASET_ID, table_id=TABLE_ID, metadata_path=testdir)
    t._refresh_templates()
    return t


@pytest.fixture(name="folder")
def fixture_folder(testdir):
    """
    Fixture for folder object.
    """
    return testdir / DATASET_ID / TABLE_ID


@pytest.fixture(name="python_path")
def fixture_python_path():
    """
    Fixture for python_path
    """
    python_path = sys.executable

    if "python" not in python_path:
        sys.exit("Cannot find Python 3 in your path")

    return python_path


@pytest.fixture(name="default_metadata_path")
def fixture_default_matadata_path():
    """
    Fixture for default_metadata_path
    """
    mt = Metadata(dataset_id=DATASET_ID, table_id=TABLE_ID)
    return mt.metadata_path


############################################################
# Conftest for new api
############################################################

API_DATASET_ID = "dados_mestres"
API_TABLE_ID = "bairro"
API_NEW_DATASET_ID = "new_dataset"
API_NEW_TABLE_ID = "new_table"
API_PUBLISH_DATASET_ID = "br_ipea_teste_avs"
API_PUBLISH_TABLE_ID = "municipios"


@pytest.fixture(name="api_dataset")
def fixture_api_dataset(testdir):
    """
    Fixture for the dataset class
    """
    config_path = Path.home() / ".basedosdados_teste"
    return Dataset(
        dataset_id=API_DATASET_ID, metadata_path=testdir, config_path=config_path
    )


@pytest.fixture(name="api_dataset_metadata_path")
def fixture_api_dataset_metadata_path(testdir):
    """
    Fixture that returns the path to the dataset metadata file.
    """
    return Path(testdir) / API_DATASET_ID


@pytest.fixture(name="api_table_metadata_path")
def fixture_api_table_metadata_path(testdir):
    """
    Fixture that returns the path to the table metadata file.
    """
    return Path(testdir) / API_DATASET_ID / API_TABLE_ID


@pytest.fixture(name="api_dataset_metadata")
def fixture_api_dataset_metadata(testdir):
    """
    Fixture that returns a `Metadata` object for the dataset.
    """
    config_path = Path.home() / ".basedosdados_teste"
    return Metadata(
        dataset_id=API_DATASET_ID, metadata_path=testdir, config_path=config_path
    )


@pytest.fixture(name="api_new_dataset_metadata")
def fixture_new_dataset_metadata(testdir):
    """
    Fixture that returns a `Metadata` object for a new dataset.
    """
    config_path = Path.home() / ".basedosdados_teste"
    return Metadata(
        dataset_id=API_NEW_DATASET_ID, metadata_path=testdir, config_path=config_path
    )


@pytest.fixture(name="api_table_metadata")
def fixture_api_table_metadata(testdir):
    """
    Fixture that returns a `Metadata` object for the table.
    """
    config_path = Path.home() / ".basedosdados_teste"
    return Metadata(
        dataset_id=API_DATASET_ID,
        table_id=API_TABLE_ID,
        metadata_path=testdir,
        config_path=config_path,
    )


@pytest.fixture(name="api_new_table_metadata")
def fixture_new_table_metadata(testdir):
    """
    Fixture that returns a `Metadata` object for a new table.
    """
    config_path = Path.home() / ".basedosdados_teste"
    return Metadata(
        dataset_id=API_NEW_DATASET_ID,
        table_id=API_NEW_TABLE_ID,
        metadata_path=testdir,
        config_path=config_path,
    )


@pytest.fixture(name="api_outdated_dataset_metadata")
def fixture_outdated_dataset_metadata(testdir):
    """
    Fixture that returns an outdated dataset `Metadata` object for a new dataset.
    """
    config_path = Path.home() / ".basedosdados_teste"
    return Metadata(
        dataset_id=API_OUTDATED_DATASET_ID,  # noqa
        metadata_path=testdir,
        config_path=config_path,
    )


@pytest.fixture(name="api_outdated_table_metadata")
def fixture_outdated_table_metadata(testdir):
    """
    Fixture that returns an outdated dataset `Metadata` object for a new table.
    """
    config_path = Path.home() / ".basedosdados_teste"
    return Metadata(
        dataset_id=API_OUTDATED_DATASET_ID,  # noqa
        table_id=API_OUTDATED_TABLE_ID,  # noqa
        metadata_path=testdir,
        config_path=config_path,
    )


@pytest.fixture(name="api_ipea_table_metadata")
def fixture_ipea_table_metadata(testdir):
    """
    Fixture to test metadata created and filled by the user.
    Args:
        testdir (str): Path to the test directory.
    Returns:
        Metadata: Metadata object for the table.
    """
    config_path = Path.home() / ".basedosdados_teste"
    return Metadata(
        dataset_id=API_PUBLISH_DATASET_ID,
        table_id=API_PUBLISH_TABLE_ID,
        metadata_path=testdir,
        config_path=config_path,
    )
