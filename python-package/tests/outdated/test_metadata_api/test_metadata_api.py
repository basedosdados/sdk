"""
Tests for the Metadata class with new API
"""

import pytest

from basedosdados import Metadata  # TODO: deprecate
from basedosdados.exceptions import BaseDosDadosException

METADATA_FILES = {"dataset": "dataset_config.yaml", "table": "table_config.yaml"}


def test_dataset_table_schema(api_table_metadata):
    """
    Test table schema.
    """

    metadata = api_table_metadata.api_data_dict

    assert metadata.get("id") == api_table_metadata.dataset_uuid
    assert metadata.get("tables")[0].get("id") == api_table_metadata.table_uuid


def test_dataset_schema(api_dataset_metadata):
    """
    Test dataset schema.
    """

    metadata = api_dataset_metadata.api_data_dict

    assert metadata.get("id") == api_dataset_metadata.dataset_uuid
    assert "themes" in metadata


def test_dataset_does_not_exists(api_new_dataset_metadata):
    """
    Test if dataset exists in API.
    """
    assert api_new_dataset_metadata.exists() is False


def test_dataset_exists(api_dataset_metadata):
    """
    Test if dataset does not exists in API.
    """
    assert api_dataset_metadata.exists() is True


def test_dataset_table_does_not_exists(api_new_dataset_metadata):
    """
    Test if table does not exists in API.
    """
    assert api_new_dataset_metadata.exists() is False


def test_dataset_table_exists(api_table_metadata):
    """
    Test if table exists in API.
    """
    assert api_table_metadata.exists() is True


@pytest.mark.skip(reason="This test needs to mock an invalid dataset.")
def test_dataset_is_not_updated(api_dataset_metadata):
    """
    Test if dataset is updated.
    TODO: Create a deterministic test to return True.
    """

    assert api_dataset_metadata.is_updated() is False


def test_columns_schema(api_table_metadata):
    """
    Test columns schema.
    """
    columns_schema = api_table_metadata.columns_schema
    assert "directory_column" in columns_schema.get("properties")


def test_table_metadata_schema(api_table_metadata):
    """
    Test metadata schema.
    """
    metadata_schema = api_table_metadata.metadata_schema
    resource_type = (
        metadata_schema.get("properties").get("resource_type").get("enum")[0]
    )
    assert resource_type == "bdm_table"


def test_dataset_metadata_schema(api_dataset_metadata):
    """
    Test metadata schema.
    """
    metadata_schema = api_dataset_metadata.metadata_schema
    resource_type = metadata_schema.get("properties").get("type").get("enum")[0]
    assert resource_type == "dataset"


def test_create_new_table(api_new_table_metadata):
    """
    Test if table is created. To be reproducible,
    the test first deletes the table if it exists.
    """
    if api_new_table_metadata.filepath.exists():
        api_new_table_metadata.filepath.unlink()

    res = api_new_table_metadata.create(
        if_exists="replace", table_only=False, columns=["ano", "sigla_uf", "dados"]
    )

    assert api_new_table_metadata.filepath.exists() is True
    assert isinstance(res, Metadata)


def test_update_dataset(api_dataset_metadata):
    """
    Test if dataset is updated.
    """
    res = api_dataset_metadata.create(if_exists="replace")
    assert isinstance(res, Metadata)


def test_update_table(api_table_metadata):
    """
    Test if table is updated.
    """
    res = api_table_metadata.create(if_exists="replace")
    assert isinstance(res, Metadata)


def test_simplify_graphql_empty_query(api_table_metadata):
    """
    Test if empty query is returned.
    """
    query = """
    """
    variables = {}
    cleaned_res = api_table_metadata._get_graphql(query, variables)
    assert cleaned_res == {}


def test_owner_org_table_exists(api_table_metadata):
    """
    Test if owner_org is returned.
    """
    owner_org = api_table_metadata.owner_org
    assert owner_org == "c0b18195-ee44-464a-8b32-dfdfd9473c4d"


def test_owner_org_dataset_exists(api_dataset_metadata):
    """
    Test if owner_org is returned.
    """
    owner_org = api_dataset_metadata.owner_org
    assert owner_org == "c0b18195-ee44-464a-8b32-dfdfd9473c4d"


def test_owner_org_new_table(api_new_table_metadata):
    """
    Test if no owner_org is returned.
    """

    with pytest.raises(BaseDosDadosException):
        _ = api_new_table_metadata.owner_org


def test_simplify_graphql_response(api_table_metadata):
    """
    Test if edges and nodes are removed from graphql response.
    """
    query_res = {
        "allDataset": {
            "edges": [
                {
                    "node": {
                        "_id": "c0b18195-ee44-464a-8b32-dfdfd9473c4d",
                        "name": "br_ibge_pib",
                        "title": "Produto Interno Bruto",
                        "description": "Produto Interno Bruto",
                        "themes": {
                            "edges": [
                                {"node": {"slug": "economia", "name": "Economia"}}
                            ]
                        },
                    }
                }
            ]
        }
    }

    cleaned_res = api_table_metadata._simplify_graphql_response(query_res)
    assert cleaned_res["allDataset"][0]["_id"] == "c0b18195-ee44-464a-8b32-dfdfd9473c4d"
    assert isinstance(cleaned_res["allDataset"][0]["themes"], list)
