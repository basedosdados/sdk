"""
Class for manage tables in Storage and BigQuery.
"""

import contextlib
import inspect
import textwrap
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional, Union

import google.api_core.exceptions
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from loguru import logger

from basedosdados.core.base import Base
from basedosdados.exceptions import BaseDosDadosException
from basedosdados.upload.connection import Connection
from basedosdados.upload.dataset import Dataset
from basedosdados.upload.datatypes import Datatype
from basedosdados.upload.storage import Storage


class Table(Base):
    """
    Manage tables in Google Cloud Storage and BigQuery.
    """

    def __init__(self, dataset_id: str, table_id: str, **kwargs):
        """
        Initializes a new instance of the class with the specified dataset and table identifiers.

        Args:
            dataset_id: The identifier of the dataset. Hyphens will be replaced with underscores.
            table_id: The identifier of the table. Hyphens will be replaced with underscores.
            **kwargs: Additional keyword arguments to be passed to the superclass initializer.

        Attributes:
            table_id: The sanitized table identifier (hyphens replaced with underscores).
            dataset_id: The sanitized dataset identifier (hyphens replaced with underscores).
            table_full_name: Dictionary containing fully qualified table names for different environments:
                - 'prod': Production BigQuery table name.
                - 'staging': Staging BigQuery table name.
                - 'all': Deep copy of the table_full_name dictionary.
        """
        super().__init__(**kwargs)

        self.table_id = table_id.replace("-", "_")
        self.dataset_id = dataset_id.replace("-", "_")
        self.table_full_name = dict(
            prod=f"{self.client['bigquery_prod'].project}.{self.dataset_id}.{self.table_id}",
            staging=f"{self.client['bigquery_staging'].project}.{self.dataset_id}_staging.{self.table_id}",
        )
        self.table_full_name.update(dict(all=deepcopy(self.table_full_name)))

    @property
    @lru_cache(256)
    def table_config(self) -> dict[str, Any]:
        """
        Load table config.
        """
        # return self._load_yaml(self.table_folder / "table_config.yaml")
        return self.backend.get_table_config(self.dataset_id, self.table_id)

    def _get_table_obj(self, mode: str):
        """
        Get table object from BigQuery.
        """

        return self.client[f"bigquery_{mode}"].get_table(
            self.table_full_name[mode]
        )

    def _is_partitioned(
        self,
        data_sample_path: Optional[Union[str, Path]] = None,
        source_format: str = "csv",
        csv_delimiter: str = ",",
    ) -> bool:
        if data_sample_path is not None:
            table_columns = self._get_columns_from_data(
                data_sample_path=data_sample_path,
                source_format=source_format,
                csv_delimiter=csv_delimiter,
                mode="staging",
            )
        else:
            table_columns = self._get_columns_metadata_from_api()

        return bool(table_columns.get("partition_columns", []))

    def _load_schema_from_json(
        self,
        columns: list[dict[str, str]],
    ) -> list[SchemaField]:
        """
        Load schema from columns metadata.
        """

        schema = []

        for col in columns:
            # ref: https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.schema.SchemaField
            if col.get("name") is None:
                msg = "Columns must have a name! Check your data files for columns without name"
                raise BaseDosDadosException(msg)

            schema.append(
                SchemaField(
                    name=col.get("name"),
                    field_type=col.get("type"),
                    description=col.get("description", None),
                )
            )
        return schema

    def _load_staging_schema_from_data(
        self,
        data_sample_path: Optional[Union[str, Path]] = None,
        source_format: str = "csv",
        csv_delimiter: str = ",",
    ) -> list[SchemaField]:
        """
        Generate schema from columns metadata in data sample.
        """

        if self.table_exists(mode="staging"):
            logger.warning(
                " {object} {object_id} allready exists, replacing schema!",
                object_id=self.table_id,
                object="Table",
            )

        table_columns = self._get_columns_from_data(
            data_sample_path=data_sample_path,
            source_format=source_format,
            csv_delimiter=csv_delimiter,
            mode="staging",
        )

        return self._load_schema_from_json(
            columns=table_columns.get("columns")
        )

    def _load_schema_from_bq(self, mode: str = "staging") -> list[SchemaField]:
        """Load schema from table config

        Args:
            mode: Which dataset to create [`prod`|`staging`].

        """
        table_columns = self._get_columns_from_bq()
        columns = table_columns.get("partition_columns") + table_columns.get(
            "columns"
        )
        return self._load_schema_from_json(columns=columns)

    def _load_schema_from_api(
        self, mode: str = "staging"
    ) -> list[SchemaField]:
        """Load schema from table config

        Args:
            mode: Which dataset to create [`prod`|`staging`].

        """
        if self.table_exists(mode=mode):
            logger.warning(
                " {object} {object_id} allready exists, replacing schema!",
                object_id=self.table_id,
                object="Table",
            )

        table_columns = self._get_columns_metadata_from_api()
        columns = table_columns.get("partition_columns") + table_columns.get(
            "columns"
        )

        return self._load_schema_from_json(columns=columns)

    def _get_columns_from_data(
        self,
        data_sample_path: Optional[Union[str, Path]] = None,
        source_format: str = "csv",
        csv_delimiter: str = ",",
        mode: str = "staging",
    ) -> dict[str, list[dict[str, str]]]:
        """
        Get the partition columns from the structure of `data_sample_path`.

        Args:
            data_sample_path: Data sample path to auto complete columns names.
                It supports Comma Delimited CSV, Apache Avro and Apache Parquet.
            source_format: Data source format. Only 'csv', 'avro' and 'parquet'
                are supported. Defaults to 'csv'.
        """

        partition_columns = []
        if isinstance(
            data_sample_path,
            (
                str,
                Path,
            ),
        ):
            # Check if partitioned and get data sample and partition columns
            data_sample_path = Path(data_sample_path)

            if data_sample_path.is_dir():
                data_sample_path = [
                    f
                    for f in data_sample_path.glob("**/*")
                    if f.is_file() and f.suffix == f".{source_format}"
                ][0]

                partition_columns = [
                    k.split("=")[0]
                    for k in data_sample_path.as_posix().split("/")
                    if "=" in k
                ]
            columns = Datatype(source_format=source_format).header(
                data_sample_path=data_sample_path, csv_delimiter=csv_delimiter
            )

        return {
            "columns": [{"name": col, "type": "STRING"} for col in columns],
            "partition_columns": [
                {"name": col, "type": "STRING"} for col in partition_columns
            ],
        }

    def _get_columns_metadata_from_api(
        self,
    ) -> dict[str, list[dict[str, str]]]:
        """
        Get columns and partition columns from API.
        """
        table_columns = self.table_config.get("columns", {})
        columns = [
            col for col in table_columns if col.get("isPartition", {}) is False
        ]

        partition_columns = [
            col for col in table_columns if col.get("isPartition", {}) is True
        ]

        return {
            "columns": [
                {
                    "name": col.get("name"),
                    "type": col.get("bigqueryType").get("name"),
                    "description": col.get("descriptionPt"),
                }
                for col in columns
            ],
            "partition_columns": [
                {
                    "name": col.get("name"),
                    "type": col.get("bigqueryType").get("name"),
                    "description": col.get("descriptionPt"),
                }
                for col in partition_columns
            ],
        }

    def _parser_blobs_to_partition_dict(self) -> Optional[dict[Any, Any]]:
        """
        Extract the partition information from the blobs.
        """

        if not self.table_exists(mode="staging"):
            return
        blobs = (
            self.client["storage_staging"]
            .bucket(self.bucket_name)
            .list_blobs(prefix=f"staging/{self.dataset_id}/{self.table_id}/")
        )
        partitions_dict = {}
        # only needs the first bloob
        for blob in blobs:
            for folder in blob.name.split("/"):
                if "=" in folder:
                    key = folder.split("=")[0]
                    value = folder.split("=")
                    try:
                        partitions_dict[key].append(value)
                    except KeyError:
                        partitions_dict[key] = [value]
            return partitions_dict

    def _get_columns_from_bq(
        self, mode: str = "staging"
    ) -> dict[str, list[dict[str, str]]]:
        """
        Get columns and partition columns from BigQuery.
        """
        if not self.table_exists(mode=mode):
            msg = f"Table {self.dataset_id}.{self.table_id} does not exist in {mode}, please create first!"
            raise BaseDosDadosException(msg)
        else:
            schema = self._get_table_obj(mode=mode).schema

        partition_dict = self._parser_blobs_to_partition_dict()

        if partition_dict:
            partition_columns = list(partition_dict.keys())
        else:
            partition_columns = []

        return {
            "columns": [
                {
                    "name": col.name,
                    "type": col.field_type,
                    "description": col.description,
                }
                for col in schema
                if col.name not in partition_columns
            ],
            "partition_columns": [
                {
                    "name": col.name,
                    "type": col.field_type,
                    "description": col.description,
                }
                for col in schema
                if col.name in partition_columns
            ],
        }  # pyright: ignore[reportReturnType]

    def _get_cross_columns_from_bq_api(self):
        """
        Get cross columns from BigQuery API.
        """
        bq = self._get_columns_from_bq(mode="staging")
        bq_columns = bq.get("partition_columns") + bq.get("columns")

        api = self._get_columns_metadata_from_api()
        api_columns = api.get("partition_columns") + api.get("columns")

        if api_columns != []:
            for bq_col in bq_columns:
                for api_col in api_columns:
                    if bq_col.get("name") == api_col.get("name"):
                        bq_col["type"] = api_col.get("type")
                        bq_col["description"] = api_col.get("description")

        return bq_columns

    def _make_publish_sql(self) -> str:
        """
        Create `publish.sql` with columns and bigquery_type.
        """

        # publish.sql header and instructions
        publish_txt = """
        /*
        Query para publicar a tabela.

        Esse é o lugar para:
            - modificar nomes, ordem e tipos de colunas
            - dar join com outras tabelas
            - criar colunas extras (e.g. logs, proporções, etc.)

        Qualquer coluna definida aqui deve também existir em `table_config.yaml`.

        # Além disso, sinta-se à vontade para alterar alguns nomes obscuros
        # para algo um pouco mais explícito.

        TIPOS:
            - Para modificar tipos de colunas, basta substituir STRING por outro tipo válido.
            - Exemplo: `SAFE_CAST(column_name AS NUMERIC) column_name`
            - Mais detalhes: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
        */
        """

        # table_columns = self._get_columns_from_api(mode="staging")

        columns = self._get_cross_columns_from_bq_api()

        # remove triple quotes extra space
        publish_txt = inspect.cleandoc(publish_txt)
        publish_txt = textwrap.dedent(publish_txt)

        # add create table statement
        project_id_prod = self.client["bigquery_prod"].project
        publish_txt += f"\n\nCREATE OR REPLACE VIEW {project_id_prod}.{self.dataset_id}.{self.table_id} AS\nSELECT \n"

        # sort columns by is_partition, partitions_columns come first

        # add columns in publish.sql
        for col in columns:
            name = col.get("name")
            bigquery_type = (
                "STRING"
                if col.get("type") is None
                else col.get("type").upper()
            )

            publish_txt += f"SAFE_CAST({name} AS {bigquery_type}) {name},\n"
        # remove last comma
        publish_txt = publish_txt[:-2] + "\n"

        # add from statement
        project_id_staging = self.client["bigquery_staging"].project
        publish_txt += f"FROM {project_id_staging}.{self.dataset_id}_staging.{self.table_id} AS t"

        return publish_txt

    def table_exists(self, mode: str) -> bool:
        """
        Check if table exists in BigQuery.

        Args:
            mode: Which dataset to check [`prod`|`staging`].
        """

        try:
            ref = self._get_table_obj(mode=mode)
        except google.api_core.exceptions.NotFound:
            ref = None

        return bool(ref)

    def _get_biglake_connection(
        self,
        set_biglake_connection_permissions: bool = True,
        location: Optional[str] = None,
    ) -> Connection:
        """
        Get or create BigLake connection and set permissions if needed.
        """
        connection = Connection(
            name="biglake", location=location, mode="staging"
        )
        if not connection.exists:
            try:
                logger.info("Creating BigLake connection...")
                connection.create()
                logger.success("BigLake connection created!")
            except google.api_core.exceptions.Forbidden as exc:
                raise BaseDosDadosException(
                    "You don't have permission to create a BigLake connection. "
                    "Please contact an admin to create one for you."
                ) from exc
            except Exception as exc:
                raise BaseDosDadosException(
                    "Something went wrong while creating the BigLake connection. "
                    "Please contact an admin to create one for you."
                ) from exc
        if set_biglake_connection_permissions:
            try:
                logger.info(
                    "Setting permissions for BigLake service account..."
                )
                connection.set_biglake_permissions()
                logger.success("Permissions set successfully!")
            except google.api_core.exceptions.Forbidden as exc:
                raise BaseDosDadosException(
                    "Could not set permissions for BigLake service account. "
                    "Please make sure you have permissions to grant roles/storage.objectViewer"
                    f" to the BigLake service account. ({connection.service_account})."
                    " If you don't, please ask an admin to do it for you or set "
                    "set_biglake_connection_permissions=False."
                ) from exc
            except Exception as exc:
                raise BaseDosDadosException(
                    "Something went wrong while setting permissions for BigLake service account. "
                    "Please make sure you have permissions to grant roles/storage.objectViewer"
                    f" to the BigLake service account. ({connection.service_account})."
                    " If you don't, please ask an admin to do it for you or set "
                    "set_biglake_connection_permissions=False."
                ) from exc

        return connection

    def _get_table_description(self, mode: str = "staging") -> str:
        """
        Get table description to BigQuery table.

        Args:
            mode: Which dataset to check [`prod`|`staging`].
        """
        table_path = self.table_full_name["prod"]
        if mode == "staging":
            description = f"staging table for `{table_path}`"
        else:
            try:
                description = self.table_config.get("descriptionPt", "")
            except BaseException:
                logger.warning(
                    f"table {self.table_id} does not have a description in the API."
                )
                description = "description not available in the API."

        return description

    def create(
        self,
        path: Optional[Union[str, Path]] = None,
        source_format: str = "csv",
        csv_delimiter: str = ",",
        csv_skip_leading_rows: int = 1,
        csv_allow_jagged_rows: bool = False,
        if_table_exists: str = "raise",
        if_storage_data_exists: str = "raise",
        if_dataset_exists: str = "pass",
        dataset_is_public: bool = True,
        location: Optional[str] = None,
        chunk_size: Optional[int] = None,
        biglake_table: bool = False,
        set_biglake_connection_permissions: bool = True,
    ) -> None:
        """
        Creates a BigQuery table in the staging dataset.

        If a path is provided, data is automatically saved in storage,
        and a datasets folder and BigQuery location are created, in addition to
        creating the table and its configuration files.

        The new table is located at `<dataset_id>_staging.<table_id>` in
        BigQuery.

        Data can be found in Storage at
        `<bucket_name>/staging/<dataset_id>/<table_id>/*` and is used to build
        the table.

        The following data types are supported:

        - Comma-Delimited CSV
        - Apache Avro
        - Apache Parquet

        Data can also be partitioned following the Hive partitioning scheme
        `<key1>=<value1>/<key2>=<value2>`. For example,
        `year=2012/country=BR`. The partition is automatically detected by
        searching for `partitions` in the `table_config.yaml` file.

        Args:
            path: The path to the file to be uploaded to create the table.
            source_format: The format of the data source. Only 'csv', 'avro',
                and 'parquet' are supported. Defaults to 'csv'.
            csv_delimiter: The separator for fields in a CSV file. The
                separator can be any ISO-8859-1 single-byte character. Defaults
                to ','.
            csv_skip_leading_rows: The number of rows at the top of a CSV file
                that BigQuery will skip when loading the data. Defaults to 1.
            csv_allow_jagged_rows: Indicates if BigQuery should allow extra
                values that are not represented in the table schema. Defaults to
                False.
            if_table_exists: Determines what to do if the table already exists:
                * `raise`: Raises a Conflict exception
                * `replace`: Replaces the table
                * `pass`: Does nothing
            if_storage_data_exists: Determines what to do if the data already
                exists on your bucket:
                * `raise`: Raises a Conflict exception
                * `replace`: Replaces the table
                * `pass`: Does nothing
            if_dataset_exists: Determines what to do if the dataset already
                exists:
                * `raise`: Raises a Conflict exception
                * `replace`: Replaces the dataset
                * `pass`: Does nothing
            dataset_is_public: Controls if the prod dataset is public or not. By
                default, staging datasets like `dataset_id_staging` are not
                public.
            location: The location of the dataset data. List of possible region
                names: [BigQuery locations](https://cloud.google.com/bigquery/docs/locations)
            chunk_size: The size of a chunk of data whenever iterating (in
                bytes). This must be a multiple of 256 KB per the API
                specification. If not specified, the chunk_size of the blob
                itself is used. If that is not specified, a default value of 40
                MB is used.
            biglake_table: Sets this as a BigLake table. BigLake tables allow
                end-users to query from external data (such as GCS) even if they
                don't have access to the source data. IAM is managed like any
                other BigQuery native table. See
                [BigLake intro](https://cloud.google.com/bigquery/docs/biglake-intro)
                for more on BigLake.
            set_biglake_connection_permissions: If set to `True`, attempts to
                grant the BigLake connection service account access to the
                table's data in GCS.
        """

        if path is None:
            # Look if table data already exists at Storage
            data = self.client["storage_staging"].list_blobs(
                self.bucket_name,
                prefix=f"staging/{self.dataset_id}/{self.table_id}",
            )

            # Raise: Cannot create table without external data
            if not data:
                raise BaseDosDadosException(
                    "You must provide a path for uploading data"
                )

        # Add data to storage
        if isinstance(
            path,
            (
                str,
                Path,
            ),
        ):
            Storage(
                dataset_id=self.dataset_id,
                table_id=self.table_id,
                config_path=self.config_path,
                bucket_name=self.bucket_name,
            ).upload(
                path=path,
                mode="staging",
                if_exists=if_storage_data_exists,
                chunk_size=chunk_size,
            )

        # Create Dataset if it doesn't exist

        dataset_obj = Dataset(
            self.dataset_id,
        )

        dataset_obj.create(
            if_exists=if_dataset_exists,
            mode="all",
            location=location,
            dataset_is_public=dataset_is_public,
        )

        if biglake_table:
            biglake_connection = self._get_biglake_connection(
                set_biglake_connection_permissions=set_biglake_connection_permissions,
                location=location,
            )
            biglake_connection_id = biglake_connection.connection_id

        table = bigquery.Table(self.table_full_name["staging"])

        table.description = self._get_table_description(mode="staging")

        table.external_data_configuration = Datatype(
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            schema=self._load_staging_schema_from_data(
                data_sample_path=path,
                source_format=source_format,
                csv_delimiter=csv_delimiter,
            ),
            source_format=source_format,
            csv_skip_leading_rows=csv_skip_leading_rows,
            csv_delimiter=csv_delimiter,
            csv_allow_jagged_rows=csv_allow_jagged_rows,
            mode="staging",
            bucket_name=self.bucket_name,
            partitioned=self._is_partitioned(
                data_sample_path=path,
                source_format=source_format,
                csv_delimiter=csv_delimiter,
            ),
            biglake_connection_id=biglake_connection_id
            if biglake_table
            else None,
        ).external_config

        # When using BigLake tables, schema must be provided to the `Table` object
        if biglake_table:
            table.schema = self._load_staging_schema_from_data(
                data_sample_path=path,
                source_format=source_format,
                csv_delimiter=csv_delimiter,
            )
            logger.info(f"Using BigLake connection {biglake_connection_id}")

        # Lookup if table alreay exists
        table_ref = None
        with contextlib.suppress(google.api_core.exceptions.NotFound):
            table_ref = self.client["bigquery_staging"].get_table(
                self.table_full_name["staging"]
            )

        if isinstance(table_ref, google.cloud.bigquery.table.Table):
            if if_table_exists == "pass":
                return None

            if if_table_exists == "raise":
                raise FileExistsError(
                    "Table already exists, choose replace if you want to overwrite it"
                )

        if if_table_exists == "replace" and self.table_exists(mode="staging"):
            self.delete(mode="staging")

        try:
            self.client["bigquery_staging"].create_table(table)
        except google.api_core.exceptions.Forbidden as exc:
            if biglake_table:
                raise BaseDosDadosException(
                    "Permission denied. The service account used to create the BigLake connection"
                    " does not have permission to read data from the source bucket. Please grant"
                    f" the service account {biglake_connection.service_account} the Storage Object Viewer"
                    " (roles/storage.objectViewer) role on the source bucket (or on the project)."
                    " Or, you can try running this again with set_biglake_connection_permissions=True."
                ) from exc
            raise BaseDosDadosException(
                "Something went wrong when creating the table. Please check the logs for more information."
            ) from exc
        except Exception as exc:
            raise BaseDosDadosException(
                "Something went wrong when creating the table. Please check the logs for more information."
            ) from exc

        logger.success(
            "{object} {object_id} was {action} in {mode}!",
            object_id=self.table_id,
            mode="staging",
            object="Table",
            action="created",
        )
        # return None

    def update(
        self,
        mode: str = "prod",
        custom_schema: Optional[list[dict[str, str]]] = None,
    ) -> None:
        """
        Updates BigQuery schema and description.

        Args:
            mode: Table of which table to update [`prod`].
            not_found_ok: What to do if table is not found.
        """

        self._check_mode(mode)

        table = self._get_table_obj(mode)

        table.description = self._get_table_description()

        # when mode is staging the table schema already exists
        if mode == "prod" and custom_schema is None:
            table.schema = self._load_schema_from_json(
                columns=self._get_cross_columns_from_bq_api()
            )
        if mode == "prod" and custom_schema is not None:
            table.schema = self._load_schema_from_json(custom_schema)

        fields = ["description", "schema"]

        self.client["bigquery_prod"].update_table(table, fields=fields)

        logger.success(
            " {object} {object_id} was {action} in {mode}!",
            object_id=self.table_id,
            mode=mode,
            object="Table",
            action="updated",
        )

    def publish(
        self,
        if_exists: str = "raise",
        custom_publish_sql: Optional[str] = None,
        custom_schema: Optional[list[dict[str, str]]] = None,
    ) -> None:
        """
        Creates BigQuery table at production dataset.

        Table should be located at `<dataset_id>.<table_id>`.

        It creates a view that uses the query from
        `<metadata_path>/<dataset_id>/<table_id>/publish.sql`.

        Make sure that all columns from the query also exist at
        `<metadata_path>/<dataset_id>/<table_id>/table_config.sql`, including
        the partitions.

        Args:
            if_exists: What to do if table exists.
                * `raise`: Raises Conflict exception
                * `replace`: Replace table
                * `pass`: Do nothing
        """
        # TODO: review this method. Check if all required fields are filled

        if if_exists == "replace" and self.table_exists(mode="prod"):
            self.delete(mode="prod")

        publish_sql = self._make_publish_sql()

        # create view using API metadata
        if custom_publish_sql is None:
            self.client["bigquery_prod"].query(publish_sql).result()
            self.update(mode="prod")

        # create view using custon query
        if custom_publish_sql is not None:
            self.client["bigquery_prod"].query(custom_publish_sql).result()
            # update schema using a custom schema
            if custom_schema is not None:
                self.update(custom_schema=custom_schema)

        logger.success(
            " {object} {object_id} was {action}!",
            object_id=self.table_id,
            object="Table",
            action="published",
        )

    def delete(self, mode: str = "all") -> None:
        """
        Deletes table in BigQuery.

        Args:
            mode: Table of which table to delete [`prod`|`staging`].
        """

        self._check_mode(mode)

        if mode == "all":
            for m, n in self.table_full_name[mode].items():
                self.client[f"bigquery_{m}"].delete_table(n, not_found_ok=True)
                logger.info(
                    " {object} {object_id}_{mode} was {action}!",
                    object_id=self.table_id,
                    mode=m,
                    object="Table",
                    action="deleted",
                )
        else:
            self.client[f"bigquery_{mode}"].delete_table(
                self.table_full_name[mode], not_found_ok=True
            )
            logger.info(
                " {object} {object_id}_{mode} was {action}!",
                object_id=self.table_id,
                mode=mode,
                object="Table",
                action="deleted",
            )

    def append(
        self,
        filepath: Union[str, Path],
        partitions: Optional[Union[str, dict[str, str]]] = None,
        if_exists: str = "replace",
        chunk_size: Optional[int] = None,
        **upload_args,
    ) -> None:
        """
        Appends new data to existing BigQuery table.

        As long as the data has the same schema. It appends the data in the
        filepath to the existing table.

        Args:
            filepath: Where to find the file that you want to upload to create a
                table with.
            partitions: Hive structured partition as a string or dict.
                * str: `<key>=<value>/<key2>=<value2>`
                * dict: `dict(key=value, key2=value2)`
            if_exists: What to do if data with same name exists in storage.
                * `raise`: Raises Conflict exception
                * `replace`: Replace table
                * `pass`: Do nothing
            chunk_size: The size of a chunk of data whenever iterating (in
                bytes). This must be a multiple of 256 KB per the API
                specification. If not specified, the chunk_size of the blob
                itself is used. If that is not specified, a default value of 40
                MB is used.
        """
        if not self.table_exists("staging"):
            raise BaseDosDadosException(
                "You cannot append to a table that does not exist"
            )
        Storage(
            self.dataset_id,
            self.table_id,
        ).upload(
            filepath,
            mode="staging",
            partitions=partitions,
            if_exists=if_exists,
            chunk_size=chunk_size,
            **upload_args,
        )
        logger.success(
            " {object} {object_id} was {action}!",
            object_id=self.table_id,
            object="Table",
            action="appended",
        )
