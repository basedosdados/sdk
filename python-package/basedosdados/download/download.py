"""Functions for managing downloads."""

import gzip
import os
import re
import shutil
import sys
import time
from functools import lru_cache, partialmethod
from pathlib import Path
from typing import Optional, TypedDict, Union

import pandas as pd
from google.cloud import bigquery, bigquery_storage_v1, storage
from google.cloud.bigquery import client as bigquery_client
from google.cloud.storage import client as storage_client
from pandas_gbq import read_gbq
from pandas_gbq.gbq import GenericGBQException
from pydata_google_auth import cache, get_user_credentials
from pydata_google_auth.exceptions import PyDataCredentialsError

from basedosdados.constants import config
from basedosdados.core.base import Base
from basedosdados.exceptions import (
    BaseDosDadosAccessDeniedException,
    BaseDosDadosAuthorizationException,
    BaseDosDadosException,
    BaseDosDadosInvalidProjectIDException,
    BaseDosDadosNoBillingProjectIDException,
)


class _GoogleClient(TypedDict):
    bigquery: bigquery_client.Client
    storage: storage_client.Client


def _set_config_variables(
    billing_project_id: Optional[str],
    from_file: bool,
) -> tuple[str, bool]:
    """Set billing_project_id and from_file variables."""

    if (
        billing_project_id is None
        and config.billing_project_id is None
        and not from_file
    ):
        raise BaseDosDadosNoBillingProjectIDException

    # standard billing_project_id configuration
    billing_project_id = billing_project_id or config.billing_project_id
    # standard from_file configuration
    from_file = from_file or config.from_file

    return billing_project_id, from_file


def read_sql(
    query: str,
    billing_project_id: Optional[str] = None,
    from_file: bool = False,
    reauth: bool = False,
    use_bqstorage_api: bool = False,
) -> pd.DataFrame:
    """
    Load data from BigQuery using a query. Just a wrapper around
    `pandas.read_gbq`.

    Args:
        query: Valid SQL Standard Query to basedosdados.
        billing_project_id: Project that will be billed. Find your Project ID
            [here](https://console.cloud.google.com/projectselector2/home/dashboard).
        from_file: Uses the credentials from file, located in
            `~/.basedosdados/credentials/`.
        reauth: Re-authorize Google Cloud Project in case you need to change
            user or reset configurations.
        use_bqstorage_api: Use the BigQuery Storage API to download query
            results quickly, but at an increased cost. [More info](https://cloud.google.com/bigquery/docs/reference/storage/).
            To use this API, first enable it in the Cloud Console:
            [Enable API](https://console.cloud.google.com/apis/library/bigquerystorage.googleapis.com).
            You must also have the `bigquery.readsessions.create` permission on
            the project you are billing queries to.

    Returns:
        Query result as a pandas DataFrame.
    """
    billing_project_id, from_file = _set_config_variables(
        billing_project_id=billing_project_id,
        from_file=from_file,
    )

    try:
        # Set a two hours timeout
        bigquery_storage_v1.client.BigQueryReadClient.read_rows = (
            partialmethod(
                bigquery_storage_v1.client.BigQueryReadClient.read_rows,
                timeout=3600 * 2,
            )  # type: ignore
        )

        return read_gbq(
            query,
            project_id=billing_project_id,
            use_bqstorage_api=use_bqstorage_api,
            credentials=_credentials(from_file=from_file, reauth=reauth),
        )  # type: ignore
    except GenericGBQException as e:
        if "Reason: 403" in str(e):
            raise BaseDosDadosAccessDeniedException from e

        if re.match("Reason: 400 POST .* [Pp]roject[ ]*I[Dd]", str(e)):
            raise BaseDosDadosInvalidProjectIDException from e

        raise e

    except PyDataCredentialsError as e:
        raise BaseDosDadosAuthorizationException from e

    except (OSError, ValueError) as e:
        no_billing_id = "Could not determine project ID" in str(e)
        no_billing_id |= "reading from stdin while output is captured" in str(
            e,
        )
        if no_billing_id:
            raise BaseDosDadosNoBillingProjectIDException from e
        raise e


def read_table(
    dataset_id: str,
    table_id: str,
    billing_project_id: Optional[str] = None,
    query_project_id: str = "basedosdados",
    limit: Optional[int] = None,
    from_file: bool = False,
    reauth: bool = False,
    use_bqstorage_api: bool = False,
) -> pd.DataFrame:
    """
    Load data from BigQuery using `dataset_id` and `table_id`.

    Args:
        dataset_id: Dataset id available in basedosdados. It should always come
            with `table_id`.
        table_id: Table id available in `basedosdados.dataset_id`. It should
            always come with `dataset_id`.
        billing_project_id: Project that will be billed. Find your Project ID
            [here](https://console.cloud.google.com/projectselector2/home/dashboard).
        query_project_id: Which project the table lives. You can change this if
            you want to query different projects.
        limit: Number of rows to read from table.
        from_file: Uses the credentials from file, located in
            `~/.basedosdados/credentials/`.
        reauth: Re-authorize Google Cloud Project in case you need to change
            user or reset configurations.
        use_bqstorage_api: Use the BigQuery Storage API to download query
            results quickly, but at an increased cost. [More info](https://cloud.google.com/bigquery/docs/reference/storage/).
            To use this API, first enable it in the Cloud Console:
            [Enable API](https://console.cloud.google.com/apis/library/bigquerystorage.googleapis.com).
            You must also have the `bigquery.readsessions.create` permission on
            the project you are billing queries to.

    Returns:
        Query result as a pandas DataFrame.
    """
    billing_project_id, from_file = _set_config_variables(
        billing_project_id=billing_project_id,
        from_file=from_file,
    )

    query = f"""
    SELECT *
    FROM `{query_project_id}.{dataset_id}.{table_id}`"""

    if limit is not None:
        query += f" LIMIT {limit}"

    return read_sql(
        query,
        billing_project_id=billing_project_id,
        from_file=from_file,
        reauth=reauth,
        use_bqstorage_api=use_bqstorage_api,
    )


def download(
    savepath: Union[str, Path],
    query: Optional[str] = None,
    dataset_id: Optional[str] = None,
    table_id: Optional[str] = None,
    billing_project_id: Optional[str] = None,
    query_project_id: str = "basedosdados",
    limit: Optional[int] = None,
    from_file: bool = False,
    reauth: bool = False,
    compression: str = "GZIP",
) -> None:
    """
    Download table or query result from basedosdados BigQuery (or other).

    * Using a **query**:

        `download('select * from basedosdados.br_suporte.diretorio_municipios limit 10')`

    * Using **dataset_id & table_id**:

        `download(dataset_id='br_suporte', table_id='diretorio_municipios')`

    You can also add arguments to modify save parameters:

    `download(dataset_id='br_suporte', table_id='diretorio_municipios', index=False, sep='|')`


    Args:
        savepath: File path to save the result. Only supports `.csv`.
        query: Valid SQL Standard Query to basedosdados. If query is available,
            `dataset_id` and `table_id` are not required.
        dataset_id: Dataset id available in basedosdados. It should always come
            with `table_id`.
        table_id: Table id available in `basedosdados.dataset_id`. It should
            always come with `dataset_id`.
        billing_project_id: Project that will be billed. Find your Project ID
            [here](https://console.cloud.google.com/projectselector2/home/dashboard).
        query_project_id: Which project the table lives. You can change this if
            you want to query different projects.
        limit: Number of rows.
        from_file: Uses the credentials from file, located in
            `~/.basedosdados/credentials/`.
        reauth: Re-authorize Google Cloud Project in case you need to change
            user or reset configurations.
        compression: Compression type. Only `GZIP` is available for now.

    Raises:
        Exception: If either `table_id`, `dataset_id` or `query` are empty.
    """
    billing_project_id, from_file = _set_config_variables(
        billing_project_id=billing_project_id,
        from_file=from_file,
    )

    if (query is None) and ((table_id is None) or (dataset_id is None)):
        raise BaseDosDadosException(
            "Either table_id, dataset_id or query should be filled.",
        )

    client = _google_client(billing_project_id, from_file, reauth)

    # makes sure that savepath is a filepath and not a folder
    savepath = _sets_savepath(
        Path(savepath) if isinstance(savepath, str) else savepath,
    )

    # if query is not defined (so it won't be overwritten) and if
    # table is a view or external or if limit is specified,
    # convert it to a query.
    if not query and (
        not _is_table(client, dataset_id, table_id, query_project_id) or limit
    ):
        query = f"""
        SELECT *
          FROM {query_project_id}.{dataset_id}.{table_id}
        """

        if limit is not None:
            query += f" limit {limit}"

    if query:
        # sql queries produces anonymous tables, whose names
        # can be found within `job._properties`
        job = client["bigquery"].query(query)

        # views may take longer: wait for job to finish.
        _wait_for(job)

        dest_table = job._properties["configuration"]["query"][
            "destinationTable"
        ]

        project_id = dest_table["projectId"]
        dataset_id = dest_table["datasetId"]
        table_id = dest_table["tableId"]

    _direct_download(
        client,
        dataset_id,  # type: ignore
        table_id,  # type: ignore
        savepath,
        project_id,  # type: ignore
        compression,
    )


def _direct_download(
    client: _GoogleClient,
    dataset_id: str,
    table_id: str,
    savepath: Path,
    project_id: str = "basedosdados",
    compression: str = "GZIP",
    extract: bool = True,
):
    """
    Download file to disk without the requirement of loading it in memory.

    Creates a temporary file based on the `table_id` and the time of execution.
    Also creates a temporary bucket using the `dataset_id` and the time of
    execution. Moves the table to the temporary file and downloads it to disk.
    In the end, removes the temporary bucket.

    Args:
        client: BigQuery and Storage clients.
        dataset_id: Dataset id available in `project_id`.
        table_id: Table id available in `project_id.dataset_id`.
        savepath: Local path in which file should be stored in disk.
        project_id: In case you want to query another project, by default
            'basedosdados'.
        compression: Compression type to use for exported files. Can be one of
            ["NONE"|"GZIP"]. Defaults to GZIP.
        extract: Whether to extract the gzip file.

    Returns:
        None
    """
    time_hash = str(hash(time.time()))

    # Bucket names must start and end with a number or letter.
    tmp_file_name = table_id
    tmp_bucket_name = _clean_name(dataset_id + "_" + time_hash)
    blob_path = f"gs://{tmp_bucket_name}/{tmp_file_name}-*"

    # Creates temporary savepath
    tmp_savepath = savepath.parent / "tmp"
    tmp_savepath.mkdir(parents=True, exist_ok=True)

    try:
        # create temporary bucket
        _create_bucket(client, tmp_bucket_name)

        # move table to temporary file inside temporary bucket
        _move_table_to_bucket(
            client,
            dataset_id,
            table_id,
            blob_path,
            project_id,
            compression,
        )

        # download file from bucket directly to disk
        _download_blob_from_bucket(client, tmp_bucket_name, tmp_savepath)

        if compression == "GZIP" and extract:
            _gzip_extract(tmp_savepath)

        _join_files(tmp_savepath, savepath)

    except Exception as err:
        # TODO handle exceptions for 404 (not found), 403 (forbidden)
        raise Exception(err) from err
    finally:
        # delete temporary bucket (even in the case of crashing)
        _delete_bucket(client, tmp_bucket_name)
        # delete temporary savepath
        shutil.rmtree(tmp_savepath)


def _download_blob_from_bucket(
    client: _GoogleClient,
    bucket_name: str,
    savepath: Path,
) -> None:
    """
    Download a blob from a bucket to the path specified.

    Args:
        client: BigQuery and Storage clients.
        bucket_name: Name of the bucket for the file to be stored.
        savepath: Local path in which file should be stored in disk.

    Returns:
        None
    """
    bucket = client["storage"].bucket(bucket_name)
    for blob in bucket.list_blobs():
        filepath = savepath / (blob.name.split("-")[-1] + ".csv.gz")
        blob.download_to_filename(filepath)


def _create_bucket(client: _GoogleClient, bucket_name: str) -> None:
    """
    Create a new bucket in a specific location with standard storage class.

    Args:
        client: BigQuery and Storage clients.
        bucket_name: Name of the bucket to be created.

    Returns:
        None
    """
    storage_client = client["storage"]
    bucket = storage_client.bucket(bucket_name)

    # standard storage class are adequate for data
    # stored for only brief periods of time
    bucket.storage_class = "STANDARD"

    storage_client.create_bucket(bucket, location="US")


def _delete_bucket(client: _GoogleClient, bucket_name: str) -> None:
    """Forceably deletes a bucket.

    This method deletes all blobs from a bucket.

    Args:
        client: BigQuery and Storage clients.
        bucket_name: Name of the bucket to be deleted.

    Returns:
        None
    """
    MAX_BLOBS = 256

    storage_client = client["storage"]
    bucket = storage_client.get_bucket(bucket_name)

    # NOTE: force=True implementation will not delete
    # the temporary bucket if n_blobs >= 256
    n_blobs = len(list(storage_client.list_blobs(bucket_name)))
    if n_blobs >= MAX_BLOBS:
        raise Exception(
            f"""Your temporary bucket contains more than {MAX_BLOBS}. You should manually delete it (force=True will not be able to delete it.).""",
        )

    bucket.delete(force=True)


def _move_table_to_bucket(
    client: _GoogleClient,
    dataset_id: str,
    table_id: str,
    blob_path: str,
    project_id: str = "basedosdados",
    compression: str = "GZIP",
) -> None:
    """
    Move table from BigQuery to bucket.

    Args:
        client: BigQuery and Storage clients.
        dataset_id: Dataset id available in `project_id`.
        table_id: Table id available in `project_id.dataset_id`.
        blob_path: Path in bucket where the table will be stored. Called
            `destination_uri` in the documentation in the form of
            `gs://<bucket_name>/<file_name.csv>`.
        project_id: In case you want to query another project, defaults to
            'basedosdados'.
        compression: Compression type to use for exported files. Can be one of
            ["NONE"|"GZIP"].

    Returns:
        None
    """
    client_bigquery = client["bigquery"]
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.job.ExtractJobConfig(compression=compression)  # type: ignore

    # perform transfer from bq to bucket
    extract_job = client_bigquery.extract_table(
        table_ref,
        blob_path,
        location="US",
        job_config=job_config,
    )
    # wait for API results
    extract_job.result()


def _gzip_extract(savepath: Path) -> None:
    """
    Extract and replace gzip file.
    """
    try:
        for file in savepath.glob("*"):
            with gzip.open(file, "rb") as f_in:
                with open(file.with_suffix(""), "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            os.remove(file)
    except Exception as e:
        raise Exception("GZIP file could not be extracted.") from e


def _join_files(tmp_savepath: Path, savepath: Path) -> None:
    """
    Join all files in savepath.
    """
    files = list(tmp_savepath.glob("*.csv"))
    with savepath.open("a+") as targetfile:
        for i, file in enumerate(files):
            with file.open("r") as f:
                if i > 0:
                    next(f)
                for line in f:
                    targetfile.write(line)

            os.remove(file)


def _is_table(
    client: _GoogleClient,
    dataset_id: Optional[str],
    table_id: Optional[str],
    project_id: str,
) -> bool:
    """
    Check whether a `table_id` is a view or not.
    """
    if (dataset_id is None) or (table_id is None):
        return False

    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    table_ref = dataset_ref.table(table_id)

    table = client["bigquery"].get_table(table_ref)

    return table.table_type == "TABLE"


def _clean_name(string: str) -> str:
    """
    Remove anything not a number or letter.
    """
    pattern = r"[^a-zA-Z0-9]+"
    replace = ""
    return re.sub(pattern, replace, string)


def _wait_for(job: bigquery.QueryJob) -> None:
    """
    Wait for BigQuery job to finish.

    Args:
        job: `bigquery.job` object from a `bigquery.Client().query()` call.
    """
    while not job.done():
        time.sleep(1)


def _sets_savepath(savepath: Path) -> Path:
    """
    Set savepath accordingly.
    """
    if savepath.suffix == ".csv":
        # make sure that path exists
        savepath.parent.mkdir(parents=True, exist_ok=True)
    else:
        raise BaseDosDadosException(
            f"Only .csv files are supported, your filename has a diferent extension: {savepath.suffix}"
        )

    return savepath


def _credentials(
    from_file: bool = False,
    reauth: bool = False,
    scopes: list[str] = ["https://www.googleapis.com/auth/cloud-platform"],
):
    """
    Get user credentials.
    """
    if "google.colab" in sys.modules:
        from google.colab import auth  # type: ignore

        auth.authenticate_user()
        return None

    if from_file:
        return Base()._load_credentials(mode="prod")

    if reauth:
        return get_user_credentials(scopes, credentials_cache=cache.REAUTH)  # type: ignore

    return get_user_credentials(scopes)


@lru_cache(256)
def _google_client(
    billing_project_id: str,
    from_file: bool,
    reauth: bool,
) -> _GoogleClient:
    """
    Get Google Cloud client for BigQuery and Storage.
    """
    credentials = _credentials(from_file=from_file, reauth=reauth)
    return {
        "bigquery": bigquery.Client(
            credentials=credentials,
            project=billing_project_id,
        ),
        "storage": storage.Client(
            credentials=credentials,
            project=billing_project_id,
        ),
    }
