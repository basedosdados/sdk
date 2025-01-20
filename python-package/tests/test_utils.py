import os
import shutil
from glob import glob

import pandas as pd

from basedosdados.upload.utils import break_file, to_partitions

test_dir = "tests/sample_data/table"
municipio_files = "tests/sample_data/table/municipio_files"
municipio_partitioned = "tests/sample_data/table/municipio_partitioned"
sample_data = "tests/sample_data/table/municipio.csv"


def test_to_partitions():
    """
    Test the to_partitions utility.
    """
    os.makedirs(f"{test_dir}/municipio_partitioned", exist_ok=True)

    df = pd.read_csv(sample_data)
    to_partitions(df, ["ano"], f"{test_dir}/municipio_partitioned")

    # assert if the files were created from 2002 to 2011
    for i in range(2002, 2012):
        assert os.path.exists(
            f"{test_dir}/municipio_partitioned/ano={i}/data.csv"
        )


def test_break_file():
    """
    Test the break_file utility.
    """
    os.makedirs(f"{test_dir}/municipio_files", exist_ok=True)
    # copy municipio.csv to municipio_files
    shutil.copy(sample_data, f"{test_dir}/municipio_files")
    # get path as string
    path = str(f"{test_dir}/municipio_files/municipio.csv")
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

    output_path = str(f"{test_dir}/municipio_files/municipio")

    files = glob(output_path + "*.csv")

    # check if the summed rows number of the files is equal to the original file
    assert sum([len(pd.read_csv(file)) for file in files]) == len(
        pd.read_csv(sample_data)
    )

    # remove municipio folder
    shutil.rmtree(municipio_files)
    shutil.rmtree(municipio_partitioned)
