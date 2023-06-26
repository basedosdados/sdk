"""
Importing the module will automatically import the submodules.
"""

import sys
import os

from basedosdados._version import __version__
from basedosdados._warnings import show_warnings

show_warnings()

sys.path.append(f"{os.getcwd()}/python-package")

# pylint: disable=C0413

from basedosdados.backend import Backend
from basedosdados.constants import constants, config
from basedosdados.upload.connection import Connection
from basedosdados.upload.dataset import Dataset
from basedosdados.upload.storage import Storage
from basedosdados.upload.table import Table
from basedosdados.download.base import reauth
from basedosdados.download.download import (
    read_sql,
    download,
    read_table,
)
from basedosdados.download.metadata import (
    list_datasets,
    list_dataset_tables,
    get_table_description,
    get_dataset_description,
    get_table_columns,
    get_table_size,
    search,
)
