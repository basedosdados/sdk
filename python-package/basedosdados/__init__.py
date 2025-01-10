"""
Importing the module will automatically import the submodules.
"""

import os
import sys

from basedosdados._version import __version__
from basedosdados._warnings import show_warnings
from basedosdados.backend import Backend
from basedosdados.constants import config, constants
from basedosdados.core.base import Base
from basedosdados.download.download import download, read_sql, read_table
from basedosdados.download.metadata import (
    get_columns,
    get_datasets,
    get_tables,
    search,
)
from basedosdados.upload.connection import Connection
from basedosdados.upload.dataset import Dataset
from basedosdados.upload.storage import Storage
from basedosdados.upload.table import Table

show_warnings()

sys.path.append(f"{os.getcwd()}/python-package")

__all__ = [
    "__version__",
    "Backend",
    "config",
    "constants",
    "Base",
    "download",
    "read_sql",
    "read_table",
    "get_columns",
    "get_datasets",
    "get_tables",
    "search",
    "Connection",
    "Dataset",
    "Storage",
    "Table",
]
