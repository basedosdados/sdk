"""Utilities functions for Upload sub-module"""

import os
from pathlib import Path
from typing import List

import pandas as pd


def to_partitions(
    data: pd.DataFrame, partition_columns: List[str], savepath: str
):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions
    Exemple:
        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }
        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):
        savepath = Path(savepath)

        # create unique combinations between partition columns
        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]

            # get filtered data
            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            # create folder tree
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)
            file_filter_save_path = Path(filter_save_path) / "data.csv"

            # append data to csv
            df_filter.to_csv(
                file_filter_save_path,
                index=False,
                mode="a",
                header=not file_filter_save_path.exists(),
            )
    else:
        raise BaseException("Data need to be a pandas DataFrame")


def break_file(filepath: str, columns: list, chunksize: int = 1000000) -> None:
    """
    Break a file into smaller files, given a list of columns to be used as a key.
    Args:
        filepath (str, pathlib.PosixPath): file path to be broken.
        columns (list): list of columns to be used as a key.
        chunksize (int): number of rows to be read at a time.
    Returns:
        None
    """
    reader = pd.read_csv(filepath, chunksize=chunksize, usecols=columns)
    for i, chunk in enumerate(reader):
        folder = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        subfolder = os.path.join(folder, filename.split(".")[0])
        # create subfolder
        if not os.path.exists(subfolder):
            os.makedirs(subfolder)
        # save chunk
        chunk.to_csv(
            os.path.join(subfolder, f'{filename.split(".")[0]}_{i}.csv'),
            index=False,
        )
