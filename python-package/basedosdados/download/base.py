'''
Functions for manage auth and credentials
'''
# pylint: disable=redefined-outer-name, protected-access
from functools import lru_cache

import sys

from google.cloud import bigquery, storage
import pydata_google_auth


from basedosdados.upload.base import Base

SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
]


def reauth():
    '''
    Reauth user credentials
    '''

    pydata_google_auth.get_user_credentials(
        SCOPES, credentials_cache=pydata_google_auth.cache.REAUTH
    )


def credentials(from_file=False, reauth=False):
    '''
    Get user credentials with google.auth
    '''

    # check if is running in colab
    if "google.colab" in sys.modules:
        # authenticate from google colab

        # pylint: disable=no-name-in-module, import-outside-toplevel, import-error
        from google.colab import auth
        auth.authenticate_user()
        return None

    #Authenticate from PyData, for compatibility reasons
    if from_file:
        return Base()._load_credentials(mode="prod")

    if reauth:
        credentials_, _ = google.auth.default(
            scopes=SCOPES,
            # credentials_cache=None,
        )

    return pydata_google_auth.get_user_credentials(
            SCOPES,

    )
    return credentials_



@lru_cache(256)
def google_client(billing_project_id, from_file, reauth):
    '''
    Get Google Cloud client for bigquery and storage
    '''

    return dict(
        bigquery=bigquery.Client(
            credentials=credentials(from_file=from_file, reauth=reauth),
            project=billing_project_id,
        ),
        storage=storage.Client(
            credentials=credentials(from_file=from_file, reauth=reauth),
            project=billing_project_id,
        ),
    )
