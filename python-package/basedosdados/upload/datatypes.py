"""
Class for define external and partiton configs for each datatype
"""

import csv

import pandas as pd
from google.cloud import bigquery

from basedosdados.exceptions import BaseDosDadosMissingDependencyException

_avro_dependencies = False
# try:
#     import pandavro
#
#     _avro_dependencies = True
# except ImportError:
#     _avro_dependencies = False


class Datatype:
    """
    Manage external and partition config
    """

    def __init__(
        self,
        dataset_id="",
        table_id="",
        schema=None,
        source_format="csv",
        csv_skip_leading_rows=1,
        csv_delimiter=",",
        csv_allow_jagged_rows=False,
        mode="staging",
        bucket_name=None,
        partitioned=False,
        biglake_connection_id=None,
    ):
        self.dataset_id = dataset_id.replace("_staging", "")
        self.schema = schema
        self.source_format = source_format
        self.csv_delimiter = csv_delimiter
        self.csv_skip_leading_rows = csv_skip_leading_rows
        self.csv_allow_jagged_rows = csv_allow_jagged_rows
        self.mode = mode
        self.uri = f"gs://{bucket_name}/staging/{self.dataset_id}/{table_id}/*"
        self.partitioned = partitioned
        self.biglake_connection_id = biglake_connection_id

    def header(self, data_sample_path, csv_delimiter: str = ","):
        """
        Retrieve the header of the data sample
        """

        if self.source_format == "csv":
            # Replace 'data_sample_path' with your actual file path
            with open(data_sample_path, "r", encoding="utf-8") as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=csv_delimiter)
                return next(csv_reader)

        if self.source_format == "avro":
            # TODO: Restore support for avro format
            # See https://github.com/ynqa/pandavro/issues/56 and https://github.com/basedosdados/sdk/issues/1728
            if not _avro_dependencies:
                msg = "Handling avro file is currently not supported due to a limitation. See https://github.com/ynqa/pandavro/issues/56"
                raise BaseDosDadosMissingDependencyException(msg)
            # dataframe = pandavro.read_avro(str(data_sample_path))
            # return list(dataframe.columns.values)
        if self.source_format == "parquet":
            dataframe = pd.read_parquet(str(data_sample_path))
            return list(dataframe.columns.values)
        raise NotImplementedError(
            "Base dos Dados just supports comma separated csv and parquet files"
        )

    def partition(self):
        """
        Configure the partitioning of the table
        """
        hive_partitioning = bigquery.external_config.HivePartitioningOptions()
        hive_partitioning.mode = "STRINGS"
        hive_partitioning.source_uri_prefix = self.uri.replace("*", "")

        return hive_partitioning

    @property
    def external_config(self):
        """
        Configure the external table
        """
        if self.source_format == "csv":
            _external_config = bigquery.ExternalConfig("CSV")
            _external_config.options.skip_leading_rows = (
                self.csv_skip_leading_rows
            )
            _external_config.options.allow_quoted_newlines = True
            _external_config.options.allow_jagged_rows = True
            _external_config.autodetect = False
            _external_config.schema = self.schema
            _external_config.options.field_delimiter = self.csv_delimiter
            _external_config.options.allow_jagged_rows = (
                self.csv_allow_jagged_rows
            )
        elif self.source_format == "avro":
            _external_config = bigquery.ExternalConfig("AVRO")
        elif self.source_format == "parquet":
            _external_config = bigquery.ExternalConfig("PARQUET")
        else:
            raise NotImplementedError(
                "Base dos Dados just supports csv and parquet files"
            )
        _external_config.source_uris = self.uri
        if self.partitioned:
            _external_config.hive_partitioning = self.partition()

        if self.biglake_connection_id:
            _external_config.connection_id = self.biglake_connection_id
            # When using BigLake tables, schema must be provided to the `Table` object, not the
            # `ExternalConfig` object.
            _external_config.schema = None

        return _external_config
