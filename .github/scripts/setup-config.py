import json
import os
from pathlib import Path
import tomlkit

CONFIG_FOLDER = Path.home() / ".basedosdados"
CREDENTIALS_FOLDER = CONFIG_FOLDER / "credentials"


def setup():
    if not os.path.exists(CREDENTIALS_FOLDER):
        os.makedirs(CREDENTIALS_FOLDER)
        print(f"{CREDENTIALS_FOLDER} folder created")

    config_toml = {
        "bucket_name": "basedosdados-dev",
        "api": {"url": "https://staging.backend.basedosdados.org/api/v1/graphql"},
        "gcloud-projects": {
            "prod": {
                "credentials_path": (CREDENTIALS_FOLDER / "prod.json").as_posix(),
                "name": "basedosdados-dev",
            },
            "staging": {
                "credentials_path": (CREDENTIALS_FOLDER / "staging.json").as_posix(),
                "name": "basedosdados-dev",
            },
        },
    }

    with open(CONFIG_FOLDER / "config.toml", "w") as io:
        tomlkit.dump(config_toml, io)

    google_credentials = os.getenv("GOOGLE_CREDENTIALS")

    assert isinstance(google_credentials, str)

    credentials_json = json.loads(google_credentials)

    with open(CREDENTIALS_FOLDER / "prod.json", "w", encoding="utf-8") as f:
        json.dump(credentials_json, f, ensure_ascii=False, indent=2)

    with open(CREDENTIALS_FOLDER / "staging.json", "w", encoding="utf-8") as f:
        json.dump(credentials_json, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    setup()
