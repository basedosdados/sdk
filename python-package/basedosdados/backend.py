"""
Module for interacting with the backend.
"""

from typing import Any, Dict, Optional

from loguru import logger
from requests import get

try:
    from gql import Client, gql
    from gql.transport.requests import RequestsHTTPTransport

    _backend_dependencies = True
except ImportError:
    _backend_dependencies = False

from basedosdados.constants import constants
from basedosdados.exceptions import (
    BaseDosDadosException,
    BaseDosDadosMissingDependencyException,
)


class SingletonMeta(type):
    """Singleton Meta to avoid multiple instances of a class"""

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Backend(metaclass=SingletonMeta):
    def __init__(self, search_url: str = None, graphql_url: str = None):
        """
        Backend class to communicate with the backend.

        Args:
            graphql_url (str): URL of the GraphQL endpoint.
        """
        self.search_url: str = search_url or constants.BACKEND_SEARCH_URL.value
        self.graphql_url: str = (
            graphql_url or constants.BACKEND_GRAPHQL_URL.value
        )
        self.graphql_client: "Client" = self._get_client()

    def get_datasets(
        self,
        dataset_id: str = None,
        dataset_name: str = None,
        page: int = 1,
        page_size: int = 10,
    ):
        """
        Get a list of available datasets,
        either by `dataset_id` or `dataset_name`

        Args:
            dataset_id(str): dataset slug in google big query (gbq).
            dataset_name(str): dataset name in base dos dados metadata.

            page(int): page for pagination.
            page_size(int): page size for pagination.
            backend(Backend): backend instance, injected automatically.

        Returns:
            list[dict]: List of datasets.
        """

        query = """
            query ($first: Int!, $offset: Int!) {
              allDataset(first: $first, offset: $offset) {
                edges {
                  node {
                    slug
                    name
                    description
                    organizations {
                      edges {
                        node {
                          name
                        }
                      }
                    }
                    tags {
                      edges {
                        node {
                          name
                        }
                      }
                    }
                    themes {
                      edges {
                        node {
                          name
                        }
                      }
                    }
                    createdAt
                    updatedAt
                  }
                }
                totalCount
              }
            }
        """
        variables = {"first": page_size, "offset": (page - 1) * page_size}

        extra = None
        if dataset_id:
            extra = f'id: "{dataset_id}"'
        if dataset_name:
            extra = f'name_Icontains: "{dataset_name}"'
        if extra:
            query = query.replace("$offset)", f"$offset, {extra})")

        return self._execute_query(query, variables, page, page_size).get(
            "allDataset"
        )

    def get_tables(
        self,
        dataset_id: str = None,
        table_id: str = None,
        table_name: str = None,
        page: int = 1,
        page_size: int = 10,
    ):
        """
        Get a list of available tables,
        either by `dataset_id`, `table_id` or `table_name`

        Args:
            dataset_id(str): dataset slug in google big query (gbq).
            table_id(str): table slug in google big query (gbq).
            table_name(str): table name in base dos dados metadata.

            page(int): page for pagination.
            page_size(int): page size for pagination.
            backend(Backend): backend instance, injected automatically.

        Returns:
            list[dict]: List of tables.
        """

        query = """
            query ($first: Int!, $offset: Int!) {
                allTable(first: $first, offset: $offset) {
                    edges {
                        node {
                            slug
                            name
                            description
                            numberRows
                            numberColumns
                            uncompressedFileSize

                        }
                    }
                    totalCount
                }
            }
        """
        variables = {"first": page_size, "offset": (page - 1) * page_size}

        extra = None
        if table_id:
            extra = f'id: "{table_id}"'
        if dataset_id:
            extra = f'dataset_id: "{dataset_id}"'
        if table_name:
            extra = f'name_Icontains: "{table_name}"'
        if extra:
            query = query.replace("$offset)", f"$offset, {extra})")

        return self._execute_query(query, variables, page, page_size).get(
            "allTable"
        )

    def get_columns(
        self,
        table_id: str = None,
        column_id: str = None,
        column_name: str = None,
        page: int = 1,
        page_size: int = 10,
    ):
        """
        Get a list of available columns,
        either by `table_id`, `column_id` or `column_name`

        Args:
            table_id(str): table slug in google big query (gbq).
            column_id(str): column slug in google big query (gbq).
            column_name(str): table name in base dos dados metadata.

            page(int): page for pagination.
            page_size(int): page size for pagination.
            backend(Backend): backend instance, injected automatically.

        Returns:
            list[dict]: List of tables.
        """

        query = """
            query ($first: Int!, $offset: Int!) {
                allColumn(first: $first, offset: $offset) {
                    edges {
                        node {
                            name
                            description
                            observations
                            bigqueryType {
                                name
                            }
                        }
                    }
                    totalCount
                }
            }
        """
        variables = {"first": page_size, "offset": (page - 1) * page_size}

        extra = None
        if column_id:
            extra = f'id: "{column_id}"'
        if table_id:
            extra = f'table_id: "{table_id}"'
        if column_name:
            extra = f'name_Icontains: "{column_name}"'
        if extra:
            query = query.replace("$offset)", f"$offset, {extra})")

        return self._execute_query(query, variables, page, page_size).get(
            "allColumn"
        )

    def search(
        self, q: str = None, page: int = 1, page_size: int = 10
    ) -> list[dict]:
        """
        Search for datasets, querying all available metadata for the term `q`

        Args:
            q(str): search term.

            page(int): page for pagination.
            page_size(int): page size for pagination.
            backend(Backend): backend instance, injected automatically.

        Returns:
            dict: page of tables.
        """
        response = get(
            url=self.search_url,
            params={"q": q, "page": page, "page_size": page_size},
        )
        if response.status_code not in [200]:
            raise BaseDosDadosException(response.text)
        return response.json()

    def get_dataset_config(self, dataset_id: str) -> Dict[str, Any]:
        """
        Get dataset configuration.
        Args:
            dataset_id (str): The ID for the dataset.
        Returns:
            Dict: Dataset configuration.
        """
        query = """
            query ($dataset_id: ID!){
                allDataset(id: $dataset_id) {
                    edges {
                        node {
                            slug
                            name
                            descriptionPt
                            createdAt
                            updatedAt
                            themes {
                                edges {
                                    node {
                                        namePt
                                    }
                                }
                            }
                            tags {
                                edges {
                                    node {
                                        namePt
                                    }
                                }
                            }
                            organizations {
                                edges {
                                    node {
                                        namePt
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """
        dataset_id = self._get_dataset_id_from_name(dataset_id)
        if dataset_id:
            variables = {"dataset_id": dataset_id}
            response = self._execute_query(query=query, variables=variables)
            return self._simplify_response(response).get("allDataset")[
                "items"
            ][0]
        else:
            return {}

    def get_table_config(
        self, dataset_id: str, table_id: str
    ) -> Dict[str, Any]:
        """
        Get table configuration.
        Args:
            dataset_id (str): The ID for the dataset.
            table_id (str): The ID for the table.
        Returns:
            Dict: Table configuration.
        """

        query = """
            query ($table_id: ID!) {
                allTable(id: $table_id) {
                    edges {
                        node {
                            slug
                            dataset {
                                slug
                                organizations {
                                  edges {
                                    node {
                                      slug
                                    }
                                  }
                                }
                            }
                            namePt
                            descriptionPt
                            columns {
                                edges {
                                    node {
                                        name
                                        isInStaging
                                        isPartition
                                        descriptionPt
                                        observations
                                        bigqueryType {
                                            name
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        """
        table_id = self._get_table_id_from_name(
            gcp_dataset_id=dataset_id, gcp_table_id=table_id
        )

        if table_id:
            variables = {"table_id": table_id}
            response = self._execute_query(query=query, variables=variables)
            return self._simplify_response(response).get("allTable")["items"][
                0
            ]
        else:
            return {}

    def _get_dataset_id_from_name(self, gcp_dataset_id):
        query = """
            query ($gcp_dataset_id: String!){
                allCloudtable(gcpDatasetId: $gcp_dataset_id) {
                    edges {
                        node {
                            table {
                                dataset {
                                    _id
                                }
                            }
                        }
                    }
                }
            }
        """

        variables = {"gcp_dataset_id": gcp_dataset_id}
        response = self._execute_query(query=query, variables=variables)
        r = {} if response is None else self._simplify_response(response)
        if r.get("allCloudtable") != []:
            return (
                r.get("allCloudtable")["items"][0]
                .get("table")
                .get("dataset")
                .get("_id")
            )
        msg = f"{gcp_dataset_id} not found. Please create the metadata first in {self.graphql_url}"
        logger.info(msg)
        return None

    def _get_table_id_from_name(self, gcp_dataset_id, gcp_table_id):
        query = """
            query ($gcp_dataset_id: String!, $gcp_table_id: String!){
                allCloudtable(gcpDatasetId: $gcp_dataset_id, gcpTableId: $gcp_table_id) {
                    edges {
                        node {
                            table {
                                _id
                            }
                        }
                    }
                }
            }
        """

        if gcp_dataset_id:
            variables = {
                "gcp_dataset_id": gcp_dataset_id,
                "gcp_table_id": gcp_table_id,
            }

            response = self._execute_query(query=query, variables=variables)
            r = {} if response is None else self._simplify_response(response)
            if r.get("allCloudtable", []) != []:
                return (
                    r.get("allCloudtable")["items"][0].get("table").get("_id")
                )
        msg = f"No table {gcp_table_id} found in {gcp_dataset_id}. Please create in {self.graphql_url}"
        logger.info(msg)
        return None

    def _get_client(
        self,
        headers: Optional[Dict[str, str]] = None,
        fetch_schema_from_transport: bool = False,
    ) -> "Client":
        """
        Get a GraphQL client.

        Args:
            headers (Dict[str, str], optional): Headers to be passed to the client.
                Defaults to None.
            fetch_schema_from_transport (bool, optional): Whether to fetch the schema
                from the transport. Defaults to False.

        Returns:
            Client: GraphQL client.
        """
        if not _backend_dependencies:
            raise BaseDosDadosMissingDependencyException(
                "Optional dependencies for backend interaction are not installed. "
                'Please install basedosdados with the "upload" extra, such as:'
                "\n\npip install basedosdados[upload]"
            )
        transport = RequestsHTTPTransport(
            url=self.graphql_url, headers=headers, use_json=True
        )
        return Client(
            transport=transport,
            fetch_schema_from_transport=fetch_schema_from_transport,
        )

    def _execute_query(
        self,
        query: str,
        variables: Optional[Dict[str, str]] = None,
        client: Optional["Client"] = None,  # type: ignore
        headers: Optional[Dict[str, str]] = None,
        page: int = 1,
        page_size: int = 10,
        fetch_schema_from_transport: bool = False,
    ) -> Dict[str, Any]:
        """
        Execute a GraphQL query.

        Args:
            query (str): GraphQL query.
            variables (Dict[str, str], optional): Variables to be passed to the query. Defaults to None.
            client (Client, optional): GraphQL client. Defaults to None.
            headers (Dict[str, str], optional): Headers to be passed to the client. Defaults to
                None.
            fetch_schema_from_transport (bool, optional): Whether to fetch the schema from the
                transport. Defaults to False.

        Returns:
            Dict: GraphQL response.
        """
        if not _backend_dependencies:
            raise BaseDosDadosMissingDependencyException(
                "Optional dependencies for backend interaction are not installed. "
                'Please install basedosdados with the "upload" extra, such as:'
                "\n\npip install basedosdados[upload]"
            )

        client_ = (
            self._get_client(
                headers=headers,
                fetch_schema_from_transport=fetch_schema_from_transport,
            )
            if client is None  # type: ignore
            else self.graphql_client
        )
        try:
            response = client_.execute(gql(query), variable_values=variables)
        except Exception as e:
            logger.error(
                f"The API URL in the config.toml file may be incorrect "
                f"or the API might be temporarily unavailable!\n"
                f"Error executing query: {e}."
            )
        return self._simplify_response(response or {}, page, page_size)

    def _simplify_response(
        self, response: dict, page: int = 1, page_size: int = 10
    ) -> dict:
        """
        Simplify the graphql response

        Args:
            response: the graphql response

        Returns:
            dict: the simplified graphql response
        """
        if response is None:
            return {}
        if response == {}:
            return {}

        output_ = {}
        for key, value in response.items():
            if isinstance(value, list) and key == "edges":
                output_["items"] = [
                    self._simplify_response(v).get("node") for v in value
                ]
            elif isinstance(value, dict):
                output_[key] = self._simplify_response(value)
            else:
                output_[key] = value

        if "totalCount" in output_:
            output_["page"] = page
            output_["page_size"] = page_size
            output_["page_total"] = int(output_.pop("totalCount") / page_size)

        return output_
