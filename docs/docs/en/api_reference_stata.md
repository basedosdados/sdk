# Stata

This API consists of modules for **data requests**: for those who wish to only query data and metadata from our project (or any other project on Google Cloud).

!!! Info "All code documentation below is in English"

## Modules (Data Request)

If this is your first time using the package, type ```db basedosdados``` and verify again if the steps above were completed successfully.

The package contains 7 commands, with their functionalities described below:

| __Command__               | __Description__                                                                |
|--------------------------|--------------------------------------------------------------------------------|
| `bd_download`            | downloads data from Data Basis (DB).                                      |
| `bd_read_sql`            | downloads DB tables using specific queries.                                   |
| `bd_read_table`          | downloads DB tables using `dataset_id` and `table_id`.                       |
| `bd_list_datasets`       | lists the `dataset_id` of available datasets in `query_project_id`.           |
| `bd_list_dataset_tables` | lists `table_id` for available tables in the specified `dataset_id`.          |
| `bd_get_table_description`| shows the complete description of the DB table.                             |
| `bd_get_table_columns`   | shows the names, types, and descriptions of columns in the specified table.   |

Each command has a supporting __help file__, just open the help and follow the instructions:

```
help [command]
```
