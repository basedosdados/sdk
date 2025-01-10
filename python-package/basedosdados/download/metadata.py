"""
Functions to get metadata from BD's API
"""

from functools import wraps

from basedosdados.backend import Backend


def check_input(f):
    """Checks if the number of inputs is valid"""

    @wraps(f)
    def wrapper(*args, **kwargs):
        if sum([a is not None for a in args]) > 1:
            raise ValueError("At most one of the inputs must be non null")
        return f(*args, **kwargs)

    return wrapper


def inject_backend(f):
    """Inject backend instance if doesn't exists"""

    @wraps(f)
    def wrapper(*args, **kwargs):
        if "backend" not in kwargs:
            kwargs["backend"] = Backend()
        return f(*args, **kwargs)

    return wrapper


@check_input
@inject_backend
def get_datasets(
    dataset_id: str = None,
    dataset_name: str = None,
    page: int = 1,
    page_size: int = 10,
    backend: Backend = None,
) -> list[dict]:
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
        dict: List of datasets.
    """
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


@check_input
@inject_backend
def get_tables(
    dataset_id: str = None,
    table_id: str = None,
    table_name: str = None,
    page: int = 1,
    page_size: int = 10,
    backend: Backend = None,
) -> list[dict]:
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
        dict: List of tables.
    """

    return backend.get_tables(
        dataset_id, table_id, table_name, page, page_size
    )


@check_input
@inject_backend
def get_columns(
    table_id: str = None,
    column_id: str = None,
    columns_name: str = None,
    page: int = 1,
    page_size: int = 10,
    backend: Backend = None,
) -> list[dict]:
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
        dict: List of tables.
    """

    result = backend.get_columns(
        table_id, column_id, columns_name, page, page_size
    )
    for item in result.get("items", []) or []:
        item["bigquery_type"] = item.pop("bigqueryType", {}).get("name")
    return result


@check_input
@inject_backend
def search(
    q: str = None,
    page: int = 1,
    page_size: int = 10,
    backend: Backend = None,
) -> list[dict]:
    """
    Search for datasets, querying all available metadata for the term `q`

    Args:
        q(str): search term.

        page(int): page for pagination.
        page_size(int): page size for pagination.
        backend(Backend): backend instance, injected automatically.

    Returns:
        dict: List of datasets and metadata.
    """
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
