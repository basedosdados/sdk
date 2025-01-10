"""
Module for managing BigQuery Connections.
"""

from typing import Union

import google.auth
from google.cloud.bigquery_connection_v1.types import CloudResourceProperties
from google.cloud.bigquery_connection_v1.types.connection import (
    Connection as BQConnection,
)
from google.cloud.bigquery_connection_v1.types.connection import (
    CreateConnectionRequest,
    DeleteConnectionRequest,
    GetConnectionRequest,
)

from basedosdados.core.base import Base


class Connection(Base):
    """
    Manages BigQuery Connections.
    """

    def __init__(
        self,
        name: str,
        location: str = None,
        mode: str = "staging",
        friendly_name: str = None,
        description: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._name = name
        self._location = location or "US"
        self._mode = mode
        self._friendly_name = friendly_name
        self._description = description
        self._project = self.config["gcloud-projects"][self._mode]["name"]
        self._parent = f"projects/{self._project}/locations/{self._location}"

    @property
    def exists(self) -> bool:
        """
        Checks if connection exists.
        """
        if self.connection:
            return True
        return False

    @property
    def connection(self) -> Union[BQConnection, None]:
        """
        Returns connection object.
        """
        client = self.client[f"bigquery_connection_{self._mode}"]
        request = GetConnectionRequest(
            name=f"{self._parent}/connections/{self._name}"
        )
        try:
            return client.get_connection(request=request)
        except google.api_core.exceptions.NotFound:
            return None
        except Exception as e:
            raise e

    @property
    def connection_id(self) -> str:
        """
        Returns the connection id. The format is:
        <project_number>.<location>.<connection_name>
        """
        project_number = self._get_project_number(self._mode)
        return f"{project_number}.{self._location.lower()}.{self._name}"

    @property
    def service_account(self) -> str:
        """
        Returns the service account associated with the connection.
        """
        conn = self.connection
        if conn:
            return conn.cloud_resource.service_account_id
        raise ValueError("Connection does not exist.")

    def create(self):
        """
        Creates a new connection.
        """
        client = self.client[f"bigquery_connection_{self._mode}"]
        request = CreateConnectionRequest(
            parent=self._parent,
            connection_id=self._name,
            connection=BQConnection(
                name=self._name,
                friendly_name=self._friendly_name or self._name,
                description=self._description or self._name,
                cloud_resource=CloudResourceProperties(),
            ),
        )
        client.create_connection(request=request)

    def set_biglake_permissions(self):
        """
        Grants all needed roles to the connection service account to be able to
        access BigLake:

        - roles/storage.objectViewer (for staging)
        """
        try:
            self._grant_role(
                role="roles/storage.objectViewer",
                member=f"serviceAccount:{self.service_account}",
                mode=self._mode,
            )
        except Exception as e:
            error_message = (
                'Failed to grant "roles/storage.objectViewer" role to '
            )
            error_message += f"service account {self.service_account} "
            error_message += (
                f"for project {self._project}. Maybe you don't have "
            )
            error_message += "permissions to grant roles?"
            raise Exception(error_message) from e

    def revoke_biglake_permissions(self):
        """
        Revokes all roles from the connection service account.
        """
        try:
            self._revoke_role(
                role="roles/storage.objectViewer",
                member=f"serviceAccount:{self.service_account}",
                mode=self._mode,
            )
        except Exception as e:
            error_message = (
                'Failed to revoke "roles/storage.objectViewer" role from '
            )
            error_message += f"service account {self.service_account} "
            error_message += (
                f"for project {self._project}. Maybe you don't have "
            )
            error_message += "permissions to revoke roles?"
            raise Exception(error_message) from e

    def delete(self):
        """
        Deletes a connection.
        """
        self.revoke_biglake_permissions()
        client = self.client[f"bigquery_connection_{self._mode}"]
        request = DeleteConnectionRequest(
            name=f"{self._parent}/connections/{self._name}"
        )
        client.delete_connection(request=request)
