"""
Test for publishing with new API.
"""
import os


from pprint import pprint

import pytest

from basedosdados.exceptions import BaseDosDadosException
from basedosdados.upload.remoteapi import RemoteAPI

METADATA_FILES = {"dataset": "dataset_config.yaml", "table": "table_config.yaml"}


@pytest.mark.skip(reason="This test needs to mock an invalid token.")
def test_user_not_authorized(api_new_dataset_metadata):
    """
    Test if user is not authorized.
    """

    with pytest.raises(BaseDosDadosException):
        api_new_dataset_metadata.publish()


def make_login(obj):
    """
    Logs the user in. Password must be set as API_PASSWORD envvar.
    """
    print("\nTrying to login...")
    token = obj.load_token()
    try:
        if not obj.verify_token(token):
            raise BaseDosDadosException("Token is invalid.")
    except BaseDosDadosException:
        print("Token is invalid. Trying to refresh...")
        try:
            obj.refresh_token(token)
        except BaseDosDadosException:
            password = os.getenv("API_PASSWORD")
            if not password:
                raise ValueError("API_PASSWORD not found in environment variables.")
            new_token = obj.get_token("mauricio", password)
            obj.save_token(new_token)


def test_publish_existent_metadata_raise(api_dataset_metadata):
    """
    Test if publishing an existent metadata raises an error.
    """
    make_login(api_dataset_metadata)
    with pytest.raises(Exception):
        api_dataset_metadata.publish(if_exists="raise")


def test_publish_existent_metadata_pass(api_dataset_metadata):
    """
    Test if publishing an existent metadata passes.
    """
    make_login(api_dataset_metadata)
    res = api_dataset_metadata.publish(if_exists="pass")
    assert isinstance(res, dict)
    assert len(res) == 0


def test_publish_new_dataset(api_new_dataset_metadata):
    """
    Test if api_data_dict is a dict.
    """
    make_login(api_new_dataset_metadata)
    response = api_new_dataset_metadata.publish()
    print(response)
    assert response["result"] == "not implemented yet"


def test_publish_existent_dataset_metadata_replace(api_dataset_metadata):
    """
    Test if publishing an existent metadata replaces data in API.
    """
    make_login(api_dataset_metadata)
    response = api_dataset_metadata.publish(if_exists="replace", update_locally=True)
    print(response)
    assert response["result"] == "not implemented yet"


def test_publish_existent_table_metadata_replace(api_table_metadata):
    """
    Test if publishing an existent metadata replaces data in API.
    """
    make_login(api_table_metadata)
    if not api_table_metadata.is_updated():
        api_table_metadata.create(if_exists="replace")
    response = api_table_metadata.publish(
        if_exists="replace", all=True, update_locally=True
    )
    assert response["result"] == "not implemented yet"


def test_api_data_dict(api_dataset_metadata):
    """
    Test if api_data_dict is a dict.
    """
    dataset_dict = api_dataset_metadata.api_data_dict
    pprint(dataset_dict)
    assert isinstance(dataset_dict, dict)


def test_api_prepare_fields(api_dataset_metadata):
    """
    Test if api_data_dict returns correct fields.
    """
    make_login(api_dataset_metadata)
    remote_api = RemoteAPI(
        api_dataset_metadata.api_graphql, api_dataset_metadata.load_token()
    )
    fields = remote_api._prepare_fields(
        {"id": api_dataset_metadata.dataset_uuid, "name": "Teste do nome"},
        model="dataset",
    )
    pprint(fields)
    assert "id" in fields


def test_api_update_dataset(api_dataset_metadata):
    """
    Test if api_data_dict is a dict.
    """
    remote_api = RemoteAPI(
        api_dataset_metadata.api_graphql, api_dataset_metadata.load_token()
    )
    response = remote_api.call_action(
        "update_dataset", api_dataset_metadata.api_data_dict
    )
    pprint(response)
    assert response["result"] == "not implemented yet"


def test_api_create_dataset(api_new_dataset_metadata):
    """
    Test if api_data_dict is a dict.
    """
    remote_api = RemoteAPI(
        api_new_dataset_metadata.api_graphql, api_new_dataset_metadata.load_token()
    )
    response = remote_api.call_action(
        "create_dataset", api_new_dataset_metadata.api_data_dict
    )
    pprint(response)
    assert response["result"] == "not implemented yet"


def test_api_create_table(api_ipea_table_metadata):
    """
    Test creating table with dataset.
    Args:
        api_ipea_table_metadata (Metadata): Metadata object (copied from ineb)
    Returns:
        None
    """
    make_login(api_ipea_table_metadata)
    response = api_ipea_table_metadata.publish(
        all=True,
        if_exists="replace",
        update_locally=False,
    )
    pprint(response)
    assert response["result"] == "not implemented yet"
