"""
Module for manage dataset to the server.
"""

from functools import lru_cache

from google.api_core.exceptions import Conflict
from google.cloud import bigquery
from loguru import logger

from basedosdados.core.base import Base


class Dataset(Base):
    """
    Manage datasets in BigQuery.
    """

    def __init__(self, dataset_id, **kwargs):
        super().__init__(**kwargs)
        self.dataset_id = dataset_id.replace("-", "_")

    @property
    @lru_cache
    def dataset_config(self):
        """
        Dataset config file.
        """
        return self.backend.get_dataset_config(self.dataset_id)

    def _loop_modes(self, mode="all"):
        """
        Loop modes.
        """

        def dataset_tag(m):
            return f"_{m}" if m == "staging" else ""

        mode = ["prod", "staging"] if mode == "all" else [mode]
        return (
            {
                "client": self.client[f"bigquery_{m}"],
                "id": f"{self.client[f'bigquery_{m}'].project}.{self.dataset_id}{dataset_tag(m)}",
                "mode": m,
            }
            for m in mode
        )

    def _setup_dataset_object(self, dataset_id, location=None, mode="staging"):
        """
        Setup dataset object.
        """

        dataset = bigquery.Dataset(dataset_id)
        if mode == "staging":
            dataset_path = dataset_id.replace("_staging", "")
            description = f"staging dataset for `{dataset_path}`"
            labels = {"staging": True}
        else:
            try:
                description = self.dataset_config.get("descriptionPt", "")
                labels = {
                    tag.get("namePt"): True
                    for tag in self.dataset_config.get("tags")
                }
            except BaseException:
                logger.warning(
                    f"dataset {dataset_id} does not have a description in the API."
                )
                description = "description not available in the API."
                labels = {}

        dataset.description = description
        dataset.labels = labels
        dataset.location = location
        return dataset

    def publicize(self, mode="all", dataset_is_public=True):
        """Changes IAM configuration to turn BigQuery dataset public.

        Args:
            mode (bool): Which dataset to create [prod|staging|all].
            dataset_is_public (bool): Control if prod dataset is public or not. By default staging datasets like `dataset_id_staging` are not public.
        """

        for m in self._loop_modes(mode):
            dataset = m["client"].get_dataset(m["id"])
            entries = dataset.access_entries
            # TODO https://github.com/basedosdados/sdk/pull/1020
            # TODO if staging dataset is private, the prod view can't acess it: if dataset_is_public and "staging" not in dataset.dataset_id:
            if dataset_is_public:
                if "staging" not in dataset.dataset_id:
                    entries.extend(
                        [
                            bigquery.AccessEntry(
                                role="roles/bigquery.dataViewer",
                                entity_type="iamMember",
                                entity_id="allUsers",
                            ),
                            bigquery.AccessEntry(
                                role="roles/bigquery.metadataViewer",
                                entity_type="iamMember",
                                entity_id="allUsers",
                            ),
                            bigquery.AccessEntry(
                                role="roles/bigquery.user",
                                entity_type="iamMember",
                                entity_id="allUsers",
                            ),
                        ]
                    )
                else:
                    entries.extend(
                        [
                            bigquery.AccessEntry(
                                role="roles/bigquery.dataViewer",
                                entity_type="iamMember",
                                entity_id="allUsers",
                            ),
                        ]
                    )
                dataset.access_entries = entries
            m["client"].update_dataset(dataset, ["access_entries"])
            logger.success(
                " {object} {object_id}_{mode} was {action}!",
                object_id=self.dataset_id,
                mode=m["mode"],
                object="Dataset",
                action="publicized",
            )

    def exists(self, mode="staging"):
        """
        Check if dataset exists.
        """
        ref_dataset_id = (
            self.dataset_id if mode == "prod" else self.dataset_id + "_staging"
        )
        try:
            ref = self.client[f"bigquery_{mode}"].get_dataset(ref_dataset_id)
        except Exception:
            ref = None
        return bool(ref)

    def create(
        self,
        mode="all",
        if_exists="raise",
        dataset_is_public=True,
        location=None,
    ):
        """Creates BigQuery datasets given `dataset_id`.

        It can create two datasets:

        * `<dataset_id>` (mode = 'prod')
        * `<dataset_id>_staging` (mode = 'staging')

        If `mode` is all, it creates both.

        Args:
            mode (str): Optional. Which dataset to create [prod|staging|all].
            if_exists (str): Optional. What to do if dataset exists

                * raise : Raises Conflict exception
                * replace : Drop all tables and replace dataset
                * update : Update dataset description
                * pass : Do nothing

            dataset_is_public (bool): Control if prod dataset is public or not. By default staging datasets like `dataset_id_staging` are not public.

            location (str): Optional. Location of dataset data.
                List of possible region names locations: https://cloud.google.com/bigquery/docs/locations


        Raises:
            Warning: Dataset already exists and if_exists is set to `raise`
        """

        # Set dataset_id to the ID of the dataset to create.
        for m in self._loop_modes(mode):
            if if_exists == "replace":
                self.delete(mode=m["mode"])
            elif if_exists == "update":
                self.update(mode=m["mode"])
                continue

            # Send the dataset to the API for creation, with an explicit timeout.
            # Raises google.api_core.exceptions.Conflict if the Dataset already
            # exists within the project.
            try:
                if not self.exists(mode=m["mode"]):
                    # Construct a full Dataset object to send to the API.
                    dataset_obj = self._setup_dataset_object(
                        dataset_id=m["id"], location=location, mode=m["mode"]
                    )
                    m["client"].create_dataset(
                        dataset_obj
                    )  # Make an API request.
                    logger.success(
                        " {object} {object_id}_{mode} was {action}!",
                        object_id=self.dataset_id,
                        mode=m["mode"],
                        object="Dataset",
                        action="created",
                    )
                    # Make prod dataset public
                    self.publicize(
                        dataset_is_public=dataset_is_public, mode=m["mode"]
                    )
            except Conflict as e:
                if if_exists == "pass":
                    continue
                raise Conflict(
                    f"Dataset {self.dataset_id} already exists"
                ) from e

    def delete(self, mode="all"):
        """Deletes dataset in BigQuery. Toogle mode to choose which dataset to delete.

        Args:
            mode (str): Optional.  Which dataset to delete [prod|staging|all]
        """

        for m in self._loop_modes(mode):
            m["client"].delete_dataset(
                m["id"], delete_contents=True, not_found_ok=True
            )
            logger.info(
                " {object} {object_id}_{mode} was {action}!",
                object_id=self.dataset_id,
                mode=m["mode"],
                object="Dataset",
                action="deleted",
            )

    def update(self, mode="all", location=None):
        """Update dataset description. Toogle mode to choose which dataset to update.

        Args:
            mode (str): Optional. Which dataset to update [prod|staging|all]
            location (str): Optional. Location of dataset data.
                List of possible region names locations: https://cloud.google.com/bigquery/docs/locations

        """

        for m in self._loop_modes(mode):
            # Send the dataset to the API to update, with an explicit timeout.
            # Raises google.api_core.exceptions.Conflict if the Dataset already
            # exists within the project.
            m["client"].update_dataset(
                self._setup_dataset_object(
                    m["id"], location=location, mode=m["mode"]
                ),
                fields=["description"],
            )  # Make an API request.

            logger.success(
                " {object} {object_id}_{mode} was {action}!",
                object_id=self.dataset_id,
                mode=m["mode"],
                object="Dataset",
                action="updated",
            )
