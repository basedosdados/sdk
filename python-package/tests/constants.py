import platform

platform_name = platform.system().lower()

DATASET_ID_PREFIX = f"pytest_{platform_name}"
TABLE_ID_PREFIX = f"pytest_{platform_name}"
