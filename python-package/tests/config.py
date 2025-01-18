import platform

platform_name = platform.system().lower()

DATASET_ID = f"pytest_{platform_name}"
TABLE_ID = f"pytest_{platform_name}"
