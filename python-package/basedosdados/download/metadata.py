"""
Functions to get metadata from BD's API.
"""

from typing import Optional

from basedosdados.backend import Backend


def get_datasets(
    dataset_id: Optional[str] = None,
    dataset_name: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
    backend: Optional[Backend] = None,
) -> list[dict]:
    """
    Get a list of available datasets, either by `dataset_id` or `dataset_name`.

    Args:
        dataset_id: Dataset slug in Google BigQuery (GBQ).
        dataset_name: Dataset name in Base dos Dados metadata.
        page: Page for pagination.
        page_size: Page size for pagination.
        backend: Backend instance, injected automatically.

    Returns:
        List of datasets.
    """
    backend = Backend() if backend is None else backend
    result = backend.get_datasets(dataset_id, dataset_name, page, page_size)
    for item in result.get("items", []) or []:
        item["organization"] = item.get("organization", {}).get("name")
        item["tags"] = [
            i.get("name") for i in item.get("tags", {}).get("items")
        ]
        item["themes"] = [
            i.get("name") for i in item.get("themes", {}).get("items")
        ]
    return result


def get_tables(
    dataset_id: Optional[str] = None,
    table_id: Optional[str] = None,
    table_name: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
    backend: Optional[Backend] = None,
) -> list[dict]:
    """
    Get a list of available tables, either by `dataset_id`, `table_id` or
    `table_name`.

    Args:
        dataset_id: Dataset slug in Google BigQuery (GBQ).
        table_id: Table slug in Google BigQuery (GBQ).
        table_name: Table name in Base dos Dados metadata.
        page: Page for pagination.
        page_size: Page size for pagination.
        backend: Backend instance, injected automatically.

    Returns:
        List of tables.
    """

    backend = Backend() if backend is None else backend
    return backend.get_tables(
        dataset_id, table_id, table_name, page, page_size
    )


def get_columns(
    table_id: Optional[str] = None,
    column_id: Optional[str] = None,
    columns_name: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
    backend: Optional[Backend] = None,
) -> list[dict]:
    """
    Get a list of available columns, either by `table_id`, `column_id` or
    `column_name`.

    Args:
        table_id: Table slug in Google BigQuery (GBQ).
        column_id: Column slug in Google BigQuery (GBQ).
        column_name: Column name in Base dos Dados metadata.
        page: Page for pagination.
        page_size: Page size for pagination.
        backend: Backend instance, injected automatically.

    Returns:
        List of columns.
    """

    backend = Backend() if backend is None else backend

    result = backend.get_columns(
        table_id, column_id, columns_name, page, page_size
    )
    for item in result.get("items", []) or []:
        item["bigquery_type"] = item.pop("bigqueryType", {}).get("name")
    return result


def search(
    q: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
    backend: Optional[Backend] = None,
) -> list[dict]:
    """
    Search for datasets, querying all available metadata for the term `q`.

    Args:
        q: Search term.
        page: Page for pagination.
        page_size: Page size for pagination.
        backend: Backend instance, injected automatically.

    Returns:
        List of datasets and metadata.
    """
    backend = Backend() if backend is None else backend

    items = []
    for item in backend.search(q, page, page_size).get("results", []):
        items.append(
            {
                "slug": item.get("slug"),
                "name": item.get("name"),
                "description": item.get("description"),
                "n_tables": item.get("n_tables"),
                "n_raw_data_sources": item.get("n_raw_data_sources"),
                "n_information_requests": item.get("n_information_requests"),
                "organization": {
                    "slug": item.get("organizations", [{}])[0].get("slug"),
                    "name": item.get("organizations", [{}])[0].get("name"),
                },
            }
        )
    return items
