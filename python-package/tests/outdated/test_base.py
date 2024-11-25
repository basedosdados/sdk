"""
Tests for Base class
"""

import os
import re
import time
from pathlib import Path

from google.cloud import bigquery, storage


def test_bucket_name(base, config_file_exists, capsys):
    """
    Test the bucket_name function
    """
    if config_file_exists:
        assert isinstance(base.bucket_name, str)
    else:
        out, err = capsys.readouterr()
        assert "Apparently, that is the first time that you are using" in out


def test_client(base, config_file_exists, capsys):
    """
    Test the client function
    """
    if config_file_exists:
        client_dict = base.client
        assert isinstance(client_dict, dict)
        assert isinstance(client_dict["bigquery_prod"], bigquery.Client)
        assert isinstance(client_dict["bigquery_staging"], bigquery.Client)
        assert isinstance(client_dict["storage_staging"], storage.Client)
    else:
        out, err = capsys.readouterr()
        assert "Apparently, that is the first time that you are using" in out


def test_config(base, config_file_exists, capsys):
    """
    Test the config function
    """
    if config_file_exists:
        config_keys = base.config.keys()
        assert "metadata_path" in config_keys
        assert "bucket_name" in config_keys
        assert "templates_path" in config_keys
        assert "gcloud-projects" in config_keys
        assert "user" in config_keys
        assert "ckan" in config_keys
    else:
        out, err = capsys.readouterr()
        assert "Apparently, that is the first time that you are using" in out


def test_config_path(base, config_file_exists, capsys):
    """
    Test the config_path function
    """
    if config_file_exists:
        assert isinstance(base.config_path, Path)
    else:
        out, err = capsys.readouterr()
        assert "Apparently, that is the first time that you are using" in out


def test_main_vars(base, config_file_exists, capsys):
    """
    Test the main_vars function
    """
    if config_file_exists:
        main_vars_keys = base.main_vars.keys()
        assert "bucket_name" in main_vars_keys
        assert "metadata_path" in main_vars_keys
        assert "templates" in main_vars_keys
    else:
        out, err = capsys.readouterr()
        assert "Apparently, that is the first time that you are using" in out


def test_metadata_path(base, config_file_exists, capsys):
    """
    Test the metadata_path function
    """
    if config_file_exists:
        assert isinstance(base.metadata_path, Path)
    else:
        out, err = capsys.readouterr()
        assert "Apparently, that is the first time that you are using" in out


def test_templates(base, config_file_exists, capsys):
    """
    Test the templates_path function
    """
    if config_file_exists:
        assert isinstance(base.templates, Path)
    else:
        out, err = capsys.readouterr()
        assert "Apparently, that is the first time that you are using" in out


def test_uri(base, config_file_exists, capsys):
    """
    Test the uri function
    """
    if config_file_exists:
        assert bool(re.match(r"gs://.*", base.uri))
    else:
        out, err = capsys.readouterr()
        assert "Apparently, that is the first time that you are using" in out


############################################
# NEW API AUTHENTICATION TESTS
# a separate configuration directory is
# used for these tests (.basedosdados_teste)
############################################
def test_load_no_token(base):
    """
    Test get_token function
    """
    if base.token_file.exists():
        base.token_file.unlink()
    access_token = base.load_token().get("access")
    assert not access_token


def test_get_new_token(base):
    """
    Test get_token function
    """

    username = base.config["user"]["email"]
    # Password must be declared as an environment variable
    password = os.getenv("API_PASSWORD")
    token = base.get_token(username, password)
    try:
        base.save_token(token)
    except Exception as e:
        print(e)

    assert base.token_file.exists()
    assert base.verify_token(token) is True


def test_refresh_token(base):
    """
    Test refresh_token function.
    TODO: This test still depends on previous one. Should use a mock?
    """
    token = base.load_token()
    time.sleep(5)  # without this, the token is not refreshed
    new_token = base.refresh_token(token)
    try:
        base.save_token(new_token)
    except Exception as e:
        print(e)
    assert base.token_file.exists()
    assert token["access"] != new_token["access"]


def test_get_dataset_id_from_slug(base):
    """
    Test graphql_request function by getting the
    id (UUID) from the slug
    TODO: This test depends on a mock of the API
    """
    dataset_id = base._get_dataset_id_from_slug("dados_mestres")
    assert dataset_id == "ba8fb30a-a978-4495-875a-5f268fab4ef5"


def test_get_table_id_from_slug(base):
    """
    Test graphql_request function by getting the
    id (UUID) from the slugs of dataset and table
    TODO: This test depends on a mock of the API
    """
    table_id = base._get_table_id_from_slug("dados_mestres", "bairro")
    assert table_id == "4f536063-9938-4d95-a0d1-4ec25fc1923b"
