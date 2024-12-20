"""
Tests for the upload utilities.
"""
import os


import shutil
from glob import glob

import pandas as pd

from basedosdados.upload.utils import break_file, to_partitions, update_columns

DATASET_ID = "pytest"
TABLE_ID = "pytest"

TABLE_FILES = ["publish.sql", "table_config.yaml"]


def test_update_columns(sample_data, testdir, table):
    """
    Test the update_columns utility.
    """
    # table = bd.Table(dataset_id=DATASET_ID, table_id=TABLE_ID, metadata_path=testdir)
    table.create(
        sample_data,
        if_table_exists="replace",
        if_table_config_exists="replace",
        if_storage_data_exists="replace",
    )

    table_config_path = testdir / DATASET_ID / TABLE_ID / "table_config.yaml"
    last_modified = table_config_path.stat().st_mtime

    publish_path = testdir / DATASET_ID / TABLE_ID / "publish.sql"
    last_modified_publish = publish_path.stat().st_mtime

    path_arq = sample_data / "arquitetura_municipio.xlsx"
    # get path as string
    path_arq = str(path_arq)

    update_columns(table_obj=table, columns_config_url_or_path=path_arq)

    assert table_config_path.stat().st_mtime > last_modified
    assert publish_path.stat().st_mtime > last_modified_publish


def test_to_partitions(sample_data, testdir):
    """
    Test the to_partitions utility.
    """
    os.makedirs(testdir / "municipio_partitioned", exist_ok=True)

    df = pd.read_csv(sample_data / "municipio.csv")
    to_partitions(df, ["ano"], testdir / "municipio_partitioned")

    # assert if the files were created from 2002 to 2011
    for i in range(2002, 2012):
        assert os.path.exists(
            testdir / "municipio_partitioned" / f"ano={i}" / "data.csv"
        )


def test_break_file(sample_data, testdir):
    """
    Test the break_file utility.
    """
    os.makedirs(testdir / "municipio_files", exist_ok=True)
    # copy municipio.csv to municipio_files
    shutil.copy(sample_data / "municipio.csv", testdir / "municipio_files")
    # get path as string
    path = str(testdir / "municipio_files" / "municipio.csv")
    break_file(
        filepath=path,
        columns=[
            "ano",
            "id_municipio",
            "pib",
            "impostos_liquidos",
            "va",
            "va_agropecuaria",
        ],
        chunksize=100,
    )

    output_path = str(testdir / "municipio_files" / "municipio")

    files = glob(output_path + "*.csv")

    # check if the summed rows number of the files is equal to the original file
    assert sum([len(pd.read_csv(file)) for file in files]) == len(
        pd.read_csv(sample_data / "municipio.csv")
    )

    # remove municipio folder
    shutil.rmtree(sample_data / "municipio")
