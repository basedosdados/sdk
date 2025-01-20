"""
Class for managing the files in cloud storage.
"""

import sys
import time
import traceback
from pathlib import Path

from loguru import logger
from tqdm import tqdm

from basedosdados.core.base import Base
from basedosdados.exceptions import BaseDosDadosException

# google retryble exceptions. References: https://googleapis.dev/python/storage/latest/retry_timeout.html#module-google.cloud.storage.retry


class Storage(Base):
    """
    Manage files on Google Cloud Storage.
    """

    def __init__(self, dataset_id, table_id, **kwargs):
        super().__init__(**kwargs)

        self.bucket = self.client["storage_staging"].bucket(self.bucket_name)
        self.dataset_id = dataset_id.replace("-", "_")
        self.table_id = table_id.replace("-", "_")

    @staticmethod
    def _resolve_partitions(partitions):
        if isinstance(partitions, dict):
            return "/".join(f"{k}={v}" for k, v in partitions.items()) + "/"

        if isinstance(partitions, str):
            if partitions.endswith("/"):
                partitions = partitions[:-1]

            # If there is no partition
            if len(partitions) == 0:
                return ""

            # It should fail if there is folder which is not a partition
            try:
                # check if it fits rule
                {
                    b.split("=")[0]: b.split("=")[1]
                    for b in partitions.split("/")
                }
            except IndexError as e:
                raise Exception(
                    f"The path {partitions} is not a valid partition"
                ) from e

            return partitions + "/"

        raise Exception(
            f"Partitions format or type not accepted: {partitions}"
        )

    def _build_blob_name(self, filename, mode, partitions=None):
        """
        Builds the blob name.
        """

        # table folder
        blob_name = f"{mode}/{self.dataset_id}/{self.table_id}/"

        # add partition folder
        if partitions is not None:
            blob_name += self._resolve_partitions(partitions)

        # add file name
        blob_name += filename

        return blob_name

    def init(self, replace=False, very_sure=False):
        """Initializes bucket and folders.

        Folder should be:

        * `raw` : that contains really raw data
        * `staging` : preprocessed data ready to upload to BigQuery

        Args:
            replace (bool): Optional.
                Whether to replace if bucket already exists
            very_sure (bool): Optional.
                Are you aware that everything is going to be erased if you
                replace the bucket?

        Raises:
            Warning: very_sure argument is still False.
        """

        if replace:
            if not very_sure:
                raise Warning(
                    "\n********************************************************"
                    "\nYou are trying to replace all the data that you have "
                    f"in bucket {self.bucket_name}.\nAre you sure?\n"
                    "If yes, add the flag --very_sure\n"
                    "********************************************************"
                )
            self.bucket.delete(force=True)

        self.client["storage_staging"].create_bucket(self.bucket)

        for folder in ["staging/", "raw/"]:
            self.bucket.blob(folder).upload_from_string("")

    def upload(
        self,
        path,
        mode="all",
        partitions=None,
        if_exists="raise",
        chunk_size=None,
        **upload_args,
    ):
        """Upload to storage at `<bucket_name>/<mode>/<dataset_id>/<table_id>`. You can:

        * Add a single **file** setting `path = <file_path>`.

        * Add a **folder** with multiple files setting `path =
          <folder_path>`. *The folder should just contain the files and
          no folders.*

        * Add **partitioned files** setting `path = <folder_path>`.
          This folder must follow the hive partitioning scheme i.e.
          `<table_id>/<key>=<value>/<key2>=<value2>/<partition>.csv`
          (ex: `mytable/country=brasil/year=2020/mypart.csv`).

        *Remember all files must follow a single schema.* Otherwise, things
        might fail in the future.

        There are 6 modes:

        * `raw` : should contain raw files from datasource
        * `staging` : should contain pre-treated files ready to upload to BiqQuery
        * `header`: should contain the header of the tables
        * `auxiliary_files`: should contain auxiliary files from eache table
        * `architecture`: should contain the architecture sheet of the tables
        * `all`: if no treatment is needed, use `all`.

        Args:
            path (str or pathlib.PosixPath): Where to find the file or
                folder that you want to upload to storage

            mode (str): Folder of which dataset to update [raw|staging|header|auxiliary_files|architecture|all]

            partitions (str, pathlib.PosixPath, or dict): Optional.
                *If adding a single file*, use this to add it to a specific partition.

                * str : `<key>=<value>/<key2>=<value2>`
                * dict: `dict(key=value, key2=value2)`

            if_exists (str): Optional.
                What to do if data exists

                * 'raise' : Raises Conflict exception
                * 'replace' : Replace table
                * 'pass' : Do nothing
            chunk_size (int): Optional
                The size of a chunk of data whenever iterating (in bytes).
                This must be a multiple of 256 KB per the API specification.
                If not specified, the chunk_size of the blob itself is used. If that is not specified, a default value of 40 MB is used.

            upload_args ():
                Extra arguments accepted by [`google.cloud.storage.blob.Blob.upload_from_file`](https://googleapis.dev/python/storage/latest/blobs.html?highlight=upload_from_filename#google.cloud.storage.blob.Blob.upload_from_filename)
        """

        if (self.dataset_id is None) or (self.table_id is None):
            raise Exception("You need to pass dataset_id and table_id")

        path = Path(path)

        if path.is_dir():
            paths = [
                f
                for f in path.glob("**/*")
                if f.is_file()
                and f.suffix in [".csv", ".parquet", "parquet.gzip"]
            ]

            parts = [
                (
                    filepath.as_posix()
                    .replace(path.as_posix() + "/", "")
                    .replace(str(filepath.name), "")
                )
                for filepath in paths
            ]

        else:
            paths = [path]
            parts = [partitions or None]

        self._check_mode(mode)

        mode = (
            ["raw", "staging", "header", "auxiliary_files", "architecture"]
            if mode == "all"
            else [mode]
        )
        for m in mode:
            for filepath, part in tqdm(
                list(zip(paths, parts)), desc="Uploading files"
            ):
                blob_name = self._build_blob_name(filepath.name, m, part)

                blob = self.bucket.blob(blob_name, chunk_size=chunk_size)

                if not blob.exists() or if_exists == "replace":
                    upload_args["timeout"] = upload_args.get("timeout", None)

                    blob.upload_from_filename(str(filepath), **upload_args)

                elif if_exists == "pass":
                    pass

                else:
                    raise BaseDosDadosException(
                        f"Data already exists at {self.bucket_name}/{blob_name}. "
                        "If you are using Storage.upload then set if_exists to "
                        "'replace' to overwrite data \n"
                        "If you are using Table.create then set if_storage_data_exists "
                        "to 'replace' to overwrite data."
                    )

                logger.success(
                    " {object} {filename}_{mode} was {action}!",
                    filename=filepath.name,
                    mode=m,
                    object="File",
                    action="uploaded",
                )

    def download(
        self,
        filename="*",
        savepath: Path = Path("."),
        partitions=None,
        mode="staging",
        if_not_exists="raise",
    ):
        """Download files from Google Storage from path `mode`/`dataset_id`/`table_id`/`partitions`/`filename` and replicate folder hierarchy
        on save,

        There are 5 modes:
        * `raw` : should contain raw files from datasource
        * `staging` : should contain pre-treated files ready to upload to BiqQuery
        * `header`: should contain the header of the tables
        * `auxiliary_files`: should contain auxiliary files from eache table
        * `architecture`: should contain the architecture sheet of the tables

        You can also use the `partitions` argument to choose files from a partition

        Args:
            filename (str): Optional
                Specify which file to download. If "*" , downloads all files within the bucket folder. Defaults to "*".

            savepath (str):
                Where you want to save the data on your computer. Must be a path to a directory.

            partitions (str, dict): Optional
                If downloading a single file, use this to specify the partition path from which to download.

                * str : `<key>=<value>/<key2>=<value2>`
                * dict: `dict(key=value, key2=value2)`


            mode (str): Optional
                Folder of which dataset to update.[raw|staging|header|auxiliary_files|architecture]

            if_not_exists (str): Optional.
                What to do if data not found.

                * 'raise' : Raises FileNotFoundError.
                * 'pass' : Do nothing and exit the function

        Raises:
            FileNotFoundError: If the given path `<mode>/<dataset_id>/<table_id>/<partitions>/<filename>` could not be found or there are no files to download.
        """

        # Prefix to locate files within the bucket
        prefix = f"{mode}/{self.dataset_id}/{self.table_id}/"

        # Add specific partition to search prefix
        if partitions:
            prefix += self._resolve_partitions(partitions)

        # if no filename is passed, list all blobs within a given table
        if filename != "*":
            prefix += filename

        blob_list = list(self.bucket.list_blobs(prefix=prefix))

        # if there are no blobs matching the search raise FileNotFoundError or return
        if not blob_list:
            if if_not_exists == "raise":
                raise FileNotFoundError(f"Could not locate files at {prefix}")
            return

        # download all blobs matching the search to given savepath
        for blob in tqdm(blob_list, desc="Download Blob"):
            # parse blob.name and get the csv file name
            csv_name = blob.name.split("/")[-1]

            # build folder path replicating storage hierarchy
            blob_folder = blob.name.replace(csv_name, "")

            # replicate folder hierarchy
            savepath = Path(savepath)
            (savepath / blob_folder).mkdir(parents=True, exist_ok=True)

            # download blob to savepath
            save_file_path = savepath / blob.name

            blob.download_to_filename(filename=save_file_path)

        logger.success(
            " {object} {object_id}_{mode} was {action} at: {path}!",
            object_id=self.dataset_id,
            mode=mode,
            object="File",
            action="downloaded",
            path={str(savepath)},
        )

    def delete_file(self, filename, mode, partitions=None, not_found_ok=False):
        """Deletes file from path `<bucket_name>/<mode>/<dataset_id>/<table_id>/<partitions>/<filename>`.

        Args:
            filename (str): Name of the file to be deleted

            mode (str): Folder of which dataset to update [raw|staging|header|auxiliary_files|architecture|all]

            partitions (str, pathlib.PosixPath, or dict): Optional.
                Hive structured partition as a string or dict

                * str : `<key>=<value>/<key2>=<value2>`
                * dict: `dict(key=value, key2=value2)`

            not_found_ok (bool): Optional.
                What to do if file not found
        """

        self._check_mode(mode)

        mode = (
            ["raw", "staging", "header", "auxiliary_files", "architecture"]
            if mode == "all"
            else [mode]
        )

        for m in mode:
            blob = self.bucket.blob(
                self._build_blob_name(filename, m, partitions)
            )

            if blob.exists() or not blob.exists() and not not_found_ok:
                blob.delete()
            else:
                return

        logger.success(
            " {object} {filename}_{mode} was {action}!",
            filename=filename,
            mode=mode,
            object="File",
            action="deleted",
        )

    def delete_table(
        self, mode="staging", bucket_name=None, not_found_ok=False
    ):
        """Deletes a table from storage, sends request in batches.

        Args:
            mode (str): Folder of which dataset to update [raw|staging|header|auxiliary_files|architecture]
                Folder of which dataset to update. Defaults to "staging".

            bucket_name (str):
                The bucket name from which to delete the table. If None, defaults to the bucket initialized when instantiating the Storage object.
                (You can check it with the Storage().bucket property)

            not_found_ok (bool): Optional.
                What to do if table not found

        """

        prefix = f"{mode}/{self.dataset_id}/{self.table_id}/"

        if bucket_name is not None:
            table_blobs = list(
                self.client["storage_staging"]
                .bucket(f"{bucket_name}")
                .list_blobs(prefix=prefix)
            )

        else:
            table_blobs = list(self.bucket.list_blobs(prefix=prefix))

        if not table_blobs:
            if not_found_ok:
                return
            raise FileNotFoundError(
                f"Could not find the requested table {self.dataset_id}.{self.table_id}"
            )
        # Divides table_blobs list for maximum batch request size
        table_blobs_chunks = [
            table_blobs[i : i + 999]
            for i in range(0, len(table_blobs), 999)  # noqa
        ]

        for i, source_table in enumerate(
            tqdm(table_blobs_chunks, desc="Delete Table Chunk")
        ):
            counter = 0
            while counter < 10:
                try:
                    with self.client["storage_staging"].batch():
                        for blob in source_table:
                            blob.delete()
                    break
                except Exception:
                    print(
                        f"Delete Table Chunk {i} | Attempt {counter}: delete operation starts again in 5 seconds...",
                    )
                    time.sleep(5)
                    counter += 1
                    traceback.print_exc(file=sys.stderr)
        logger.success(
            " {object} {object_id}_{mode} was {action}!",
            object_id=self.table_id,
            mode=mode,
            object="Table",
            action="deleted",
        )

    def copy_table(
        self,
        source_bucket_name="basedosdados",
        destination_bucket_name=None,
        mode="staging",
        new_table_id=None,
    ):
        """Copies table from a source bucket to your bucket, sends request in batches.

        Args:
            source_bucket_name (str):
                The bucket name from which to copy data. You can change it
                to copy from other external bucket.

            destination_bucket_name (str): Optional
                The bucket name where data will be copied to.
                If None, defaults to the bucket initialized when instantiating the Storage object (You can check it with the
                Storage().bucket property)

            mode (str): Folder of which dataset to update [raw|staging|header|auxiliary_files|architecture]
                Folder of which dataset to update. Defaults to "staging".
            new_table_id (str): Optional.
                New table id to be copied to. If None, defaults to the table id initialized when instantiating the Storage object.
        """

        source_table_ref = list(
            self.client["storage_staging"]
            .bucket(source_bucket_name)
            .list_blobs(prefix=f"{mode}/{self.dataset_id}/{self.table_id}/")
        )

        if not source_table_ref:
            raise FileNotFoundError(
                f"Could not find the requested table {self.dataset_id}.{self.table_id}"
            )

        if destination_bucket_name is None:
            destination_bucket = self.bucket

        else:
            destination_bucket = self.client["storage_staging"].bucket(
                destination_bucket_name
            )

        # Divides source_table_ref list for maximum batch request size
        source_table_ref_chunks = [
            source_table_ref[i : i + 999]  # noqa
            for i in range(0, len(source_table_ref), 999)  # noqa
        ]

        for i, source_table in enumerate(
            tqdm(source_table_ref_chunks, desc="Copy Table Chunk")
        ):
            counter = 0
            while counter < 10:
                try:
                    with self.client["storage_staging"].batch():
                        for blob in source_table:
                            new_name = None
                            if new_table_id:
                                new_name = blob.name.replace(
                                    self.table_id, new_table_id
                                )
                            self.bucket.copy_blob(
                                blob,
                                destination_bucket=destination_bucket,
                                new_name=new_name,
                            )
                    break
                except Exception:
                    print(
                        f"Copy Table Chunk {i} | Attempt {counter}: copy operation starts again in 5 seconds...",
                    )
                    counter += 1
                    time.sleep(5)
                    traceback.print_exc(file=sys.stderr)
        logger.success(
            " {object} {object_id}_{mode} was {action} to {new_object_id}_{mode}!",
            object_id=self.table_id,
            new_object_id=new_table_id if new_table_id else self.table_id,
            mode=mode,
            object="Table",
            action="copied",
        )
