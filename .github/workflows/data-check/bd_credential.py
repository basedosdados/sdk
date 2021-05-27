import sys
import os
import shutil
import base64
import json
import yaml
import toml
import tomlkit
import traceback

from pathlib import Path
from jinja2 import Template

import basedosdados
import basedosdados as bd
from basedosdados import Storage
from basedosdados import Dataset
from basedosdados.upload.base import Base


def decoding_base64(message):
    # decoding the base64 string
    base64_bytes = message.encode("ascii")
    message_bytes = base64.b64decode(base64_bytes)
    return message_bytes.decode("ascii")


def create_config_folder(config_folder):
    ## if ~/.basedosdados folder exists delete
    if os.path.exists(Path.home() / config_folder):
        shutil.rmtree(Path.home() / config_folder, ignore_errors=True)
    ## create ~/.basedosdados folder
    os.mkdir(Path.home() / config_folder)
    os.mkdir(Path.home() / config_folder / "credentials")


def save_json(json_obj, file_path, file_name):
    ### function to save json file
    with open(f"{file_path}/{file_name}", "w", encoding="utf-8") as f:
        json.dump(json_obj, f, ensure_ascii=False, indent=2)


def create_json_file(message_base64, file_name, config_folder):
    ### decode base64 script and load as a json object
    json_obj = json.loads(decoding_base64(message_base64))
    prod_file_path = Path.home() / config_folder / "credentials"
    ### save the json credential in the .basedosdados/credentials/
    save_json(json_obj, prod_file_path, file_name)


def save_toml(config_dict, file_name, config_folder):
    ### save the config.toml in .basedosdados
    file_path = Path.home() / config_folder
    with open(file_path / file_name, "w") as toml_file:
        toml.dump(config_dict, toml_file)


def load_configs(dataset_id, table_id):
    ### get the config file in .basedosdados/config.toml
    configs_path = Base()._load_config()
    ### get the path to metadata_path, where the folder bases with metadata information
    metadata_path = configs_path["metadata_path"]
    ### get the path to table_config.yaml
    table_path = f"{metadata_path}/{dataset_id}/{table_id}"

    return (
        ### load the table_config.yaml
        yaml.load(open(f"{table_path}/table_config.yaml", "r"), Loader=yaml.FullLoader),
        ### return the path to .basedosdados configs
        configs_path,
    )


def create_config_tree(prod_base64, staging_base64, config_dict):
    ### execute the creation of .basedosdados
    create_config_folder(".basedosdados")
    ### create the prod.json secret
    create_json_file(prod_base64, "prod.json", ".basedosdados")
    ### create the staging.json secret
    create_json_file(staging_base64, "staging.json", ".basedosdados")
    ### create the config.toml
    save_toml(config_dict, "config.toml", ".basedosdados")


def pretty_log(dataset_id, table_id, source_bucket_name):

    if "basedosdados" in source_bucket_name:
        source_len = len(source_bucket_name) - 9
    else:
        source_len = len(source_bucket_name)
    print(
        "\n###================================================================================###",
        "\n###                                                                                ###",
        "\n###               Data successfully synced and created in bigquery                 ###",
        "\n###                                                                                ###",
        f"\n###               Dataset      : {dataset_id}",
        " " * (48 - len(dataset_id)),
        "###",
        f"\n###               Table        : {table_id}",
        " " * (48 - len(table_id)),
        "###",
        f"\n###               Source Bucket: {source_bucket_name}",
        " " * (48 - source_len),
        "###",
        "\n###                                                                                ###",
        "\n###================================================================================###\n",
    )


def get_table_dataset_id():
    ### load the change files in PR || diff between PR and master
    changes = json.load(Path("/github/workspace/files.json").open("r"))
    print(changes)
    ### create a dict to save the dataset and source_bucket related to each table_id
    dataset_table_ids = {}
    ### create a list to save the table folder path, for each table changed in the commit
    table_folders = []
    for change_file in changes:
        ### get the directory path for a table with changes
        file_dir = Path(change_file).parent
        ### append the table directory if it was not already appended
        if file_dir not in table_folders:
            table_folders.append(file_dir)
    ### construct the iterable for the table_config paths
    table_config_paths = [Path(root / "table_config.yaml") for root in table_folders]
    ### iterate through each config path
    for filepath in table_config_paths:
        ### check if the table_config.yaml exists in the changed folder
        if filepath.is_file():
            ### load the found table_config.yaml
            table_config = yaml.load(open(filepath, "r"), Loader=yaml.SafeLoader)
            ### add the dataset and source_bucket for each table_id
            dataset_table_ids[table_config["table_id"]] = {
                "dataset_id": table_config["dataset_id"],
                "source_bucket_name": table_config["source_bucket_name"],
                "table_config_path": filepath,
            }
        else:
            print(
                "\n###==============================================================================================###",
                f"\n{str(filepath)} does not exist on current commit",
                "\n###==============================================================================================###\n",
            )
    return dataset_table_ids, changes


def setup():
    # print(os.environ.get("INPUT_PROJECT_ID"))
    # print(Path.home())

    ### load the secret of prod and staging data
    prod_base64 = os.environ.get("INPUT_GCP_TABLE_APPROVE_PROD")
    staging_base64 = os.environ.get("INPUT_GCP_TABLE_APPROVE_STAGING")

    ### json with information of .basedosdados/config.toml
    config_dict = {
        "metadata_path": "/github/workspace/bases",
        "templates_path": "/github/workspace/python-package/basedosdados/configs/templates",
        "bucket_name": "basedosdados",
        "gcloud-projects": {
            "staging": {
                "name": "basedosdados-staging",
                "credentials_path": "/github/home/.basedosdados/credentials/staging.json",
            },
            "prod": {
                "name": "basedosdados",
                "credentials_path": "/github/home/.basedosdados/credentials/prod.json",
            },
        },
    }

    ### create config and credential folders
    create_config_tree(prod_base64, staging_base64, config_dict)

    return get_table_dataset_id()
