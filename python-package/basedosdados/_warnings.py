"""
Checks for warnings and general-purpose messages and displays them to the user.
"""

from typing import List, Tuple

import requests
from loguru import logger

from basedosdados._version import __version__


def get_latest_version_number():
    """Get the latest version number from PyPI."""
    try:
        response = requests.get(
            "https://pypi.python.org/pypi/basedosdados/json", timeout=5
        )
        return response.json()["info"]["version"]
    except:  # noqa
        logger.warning(
            "Could not check for updates. Please check your internet connection."
        )
        return None


def compare_version_numbers(
    versionA: str, versionB: str
) -> Tuple[int, str, str]:
    """
    Compares two version numbers and returns the difference between them.

    Args:
        versionA (str): The first version number.
        versionB (str): The second version number.

    Returns:
        1 if versionA > versionB
        0 if versionA == versionB
        -1 if versionA < versionB
    """

    def parse_version(version: str):
        version: List[str] = version.split(".")
        if len(version) == 1:
            version.append(0)
            version.append(0)
        elif len(version) == 2:
            version.append(0)
        elif len(version) > 3:
            version = version[:3]
        version[0] = int(version[0])
        version[1] = int(version[1])
        patch = ""
        for char in version[2]:
            if char.isdigit():
                patch += char
            else:
                break
        version_type = ""
        if "b" in version[2]:
            version_type = "BETA"
        elif "a" in version[2]:
            version_type = "ALPHA"
        else:
            version_type = "GA"
        version[2] = int(patch)
        version.append(version_type)
        return version

    versionA = parse_version(versionA)
    versionB = parse_version(versionB)

    # Compare types. If both types are the same, compare the numbers.
    if versionA[3] == versionB[3]:
        for i in range(3):
            if versionA[i] > versionB[i]:
                return 1, versionA[3], versionB[3]
            elif versionA[i] < versionB[i]:
                return -1, versionA[3], versionB[3]
        return 0, versionA[3], versionB[3]
    # If the types are different, don't compare the numbers.
    return 0, versionA[3], versionB[3]


def show_warnings():
    """Show warnings and general-purpose messages to the user."""
    # Version warning
    try:
        latest_version = get_latest_version_number()
        if latest_version is not None:
            comparison = compare_version_numbers(__version__, latest_version)
            if comparison[0] == -1:
                logger.warning(
                    f"You are using an outdated version of basedosdados ({__version__}). "
                    f"Please upgrade to the latest version ({latest_version}) using "
                    "'pip install --upgrade basedosdados'."
                )
    except:  # noqa
        logger.warning(
            "Could not check for updates. Please check your internet connection."
        )
    # General-purpose warnings and messages
    try:
        response = requests.get(
            "https://basedosdados.github.io/notifications/data.json"
        )
        data = response.json()
        if "general" in data:
            if "messages" in data["general"]:
                for message in data["general"]["messages"]:
                    logger.info(message)
            if "warnings" in data["general"]:
                for warning in data["general"]["warnings"]:
                    logger.warning(warning)
        if "python" in data:
            if "messages" in data["python"]:
                for message in data["python"]["messages"]:
                    logger.info(message)
            if "warnings" in data["python"]:
                for warning in data["python"]["warnings"]:
                    logger.warning(warning)
    except:  # noqa
        logger.warning(
            "Could not check for warnings and messages. Please check your internet connection."
        )
