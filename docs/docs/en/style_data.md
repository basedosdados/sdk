# Style Guide

In this section we list all the standards from our style guide and data guidelines that we use at Data Basis. They help us maintain high quality in the data and metadata we publish.

!!! Tip "You can use the left menu to navigate through the different topics on this page."

---

## Naming datasets and tables

### Datasets (`dataset_id`)

We name datasets in the format `<organization_id>_<description>`, where
`organization_id` follows by default the **geographic scope of the
organization** that publishes the dataset:

|           | organization_id                         |
|-----------|----------------------------------------------|
| Global    | `world_<organization>`                         |
| Federal   | `<country_code>_<organization>`                         |
| State     | `<country_code>_<state_code>_<organization>`             |
| Municipal | `<country_code>_<state_code>_<city>_<organization>`   |

* `country_code` and `state_code` are always 2 lowercase letters;
* `organization` is the name or acronym (preferably) of the organization that
  published the original data (e.g., `ibge`, `tse`, `inep`).
* `description` is a brief description of the dataset

For example, the GDP dataset from IBGE has as `dataset_id`: `br_ibge_pib`

!!! Tip "Not sure how to name the organization?"
    We suggest visiting their website and seeing how they refer to themselves (e.g., DETRAN-RJ would be `br_rj_detran`)

### Tables

Naming tables is less structured and therefore requires good judgment. But we have some rules:

- If there are tables for different entities, include the entity at the beginning of the name. Example: `municipality_value`, `state_value`.
- Do not include the time unit in the name. Example: name it `municipality`, not `municipality_year`.
- Keep names in singular. Example: `school`, not `schools`.
- Name the most disaggregated tables as `microdata`. Generally these have data at the person or transaction level.

### Examples of `dataset_id.table_id`

|           |                                           |                                                     |
|-----------|-------------------------------------------|-----------------------------------------------------|
| Global    | `world_waze.alerts`                      | Waze alert data from different cities.    |
| Federal   | `br_tse_elections.candidates`              | TSE political candidate data.      |
| Federal   | `br_ibge_pnad.microdata`                 | Microdata from the National Household Sample Survey produced by IBGE. |
| Federal   | `br_ibge_pnadc.microdata`                | Microdata from the Continuous National Household Sample Survey (PNAD-C) produced by IBGE. |
| State     | `br_sp_see_teachers.workload`        | Anonymized workload of active teachers in SP state education network. |
| Municipal | `br_rj_riodejaneiro_cmrj_legislative.votes` | Voting data from Rio de Janeiro City Council (RJ). |

## Table formats

Tables should, whenever possible, be in `long` format, rather than `wide`.

## Variable naming

Variable names must follow some rules:

- Use existing names in the repository as much as possible. Examples: `year`, `month`, `municipality_id`, `state_code`, `age`, `position`, `result`, `votes`, `revenue`, `expense`, `price`, etc.
- Respect directory table patterns.
- Be as intuitive, clear, and extensive as possible.
- Have all lowercase letters (including acronyms), without accents, connected by `_`.
- Do not include connectors like `of`, `the`, `and`, `in`, etc.
- Only have the `id_` prefix when the variable represents primary keys of entities (that would eventually have a directory table).
    - Examples that have it: `municipality_id`, `state_id`, `school_id`, `person_id`.
    - Examples that don't: `network`, `location`.
    - **Important**: when the dataset is in English, id becomes a suffix
- Only have entity suffixes when the column's entity is different from the table's entity.
    - Examples that have it: in a table with entity `person`, a column about municipal GDP would be called `municipality_gdp`.
    - Examples that don't: in a table with entity `person`, person characteristics would be called `name`, `age`, `sex`, etc.
- List of **allowed prefixes**
    - `name_`,
    - `date_`,
    - `number_`,
    - `quantity_`,
    - `proportion_` (percentage variables 0-100%),
    - `rate_`,
    - `ratio_`,
    - `index_`,
    - `indicator_` (boolean type variables),
    - `type_`,
    - `code_`,
    - `sequential_`.
- List of **common suffixes**
    - `_pc` (per capita)

## Measurement units

The rule is to keep variables with their original measurement units listed in this [code](https://github.com/basedosdados/website/blob/master/ckanext-basedosdados/ckanext/basedosdados/validator/available_options/measurement_unit.py), with the exception of financial variables where we convert old currencies to current ones (e.g. Cruzeiro to Real).

We catalog measurement units in standard format in the architecture table. [Complete list here](https://github.com/basedosdados/website/blob/master/ckanext-basedosdados/ckanext/basedosdados/validator/available_options/measurement_unit.py) Examples: `m`, `km/h`, `BRL`.

For deflated financial columns, we list the currency with the base year. Example: a column measured in reais in 2010 has unit `BRL_2010`.

Variables must always have measurement units with base 1. In other words, having `BRL` instead of `1000 BRL`, or `person` instead of `1000 persons`. This information, as well as other column metadata, is recorded in the architecture table of the table.

## Which variables to keep, add, and remove

We partially [normalize](https://www.guru99.com/database-normalization.html) our tables and have rules for which variables to include in production. They are:

- Remove variables from entity names that already exist in directories. Example: remove `municipality` from the table that already includes `municipality_id`.
- Remove variables serving as partitions. Example: remove `year` and `state_code` if the table is partitioned in these two dimensions.
- Add primary keys for each existing entity. Example: add `municipality_id` to tables that only include `municipality_tse`.
- Keep all primary keys that already come with the table, but (1) add relevant keys (e.g. `state_code`, `municipality_id`) and (2) remove irrelevant keys (e.g. `region`).

## Temporal coverage

Fill in the `temporal_coverage` column in table, column, and key metadata (in dictionaries) according to the following pattern.

- General format: `initial_date(temporal_unit)final_date`
    - `initial_date` and `final_date` are in the corresponding temporal unit.
        - Example: table with unit `year` has coverage `2005(1)2018`.
        - Example: table with unit `month` has coverage `2005-08(1)2018-12`.
        - Example: table with unit `week` has coverage `2005-08-01(7)2018-08-31`.
        - Example: table with unit `day` has coverage `2005-08-01(1)2018-12-31`.

- Rules for filling in
    - Table metadata
        - Fill in the general format.
    - Column metadata
        - Fill in the general format, except when `initial_date` or `final_date` are equal to the table's. In that case, leave it empty.
        - Example: suppose the table's coverage is `2005(1)2018`.
            - If a column appears only in 2012 and exists until 2018, we fill in its coverage as `2012(1)`.
            - If a column disappears in 2013, we fill in its coverage as `(1)2013`.
            - If a column exists in the same temporal coverage as the table, we fill in its coverage as `(1)`.
    - Key metadata
        - Fill in the same pattern of columns, but with the reference being the corresponding column, not the table.

## Cleaning STRINGs

- Categorical variables: initial uppercase and rest lowercase, with accents.
- Unstructured STRINGs: keep them as they are.

## Value formats

- Decimal: American format, i.e., always `.` (dot) instead of `,` (comma).
- Date: `YYYY-MM-DD`
- Time (24h): `HH:MM:SS`
- Datetime ([ISO-8601](https://en.wikipedia.org/wiki/ISO_8601)): `YYYY-MM-DDTHH:MM:SS.sssZ`
- Null value: `""` (csv), `NULL` (Python), `NA` (R), `.` or `""` (Stata)
- Proportion/percentage: between 0-100

## Table partitioning

### **What is partitioning and what is its goal?**

In a nutshell, partitioning a table is dividing it into multiple blocks/parts. The central objective is to reduce financial costs and increase performance, as the larger the volume of data, the greater the storage and query costs.

The reduction in costs and the increase in performance mainly occur because partitioning allows the data set to be reorganized into small **grouped blocks**. In practice, by performing the partitioning, it is possible to avoid that a query traverses the entire table just to bring a small data slice.

An example of our beloved RAIS:

- Without using partition filtering:

For this case, Bigquery scanned all (*) columns and rows of the dataset. It's worth noting that this cost is still not very large, as the base has already been partitioned. If this dataset hadn't passed through the partition process, this query would have cost a lot more money and time, as it involves a considerable volume of data.

![image](https://user-images.githubusercontent.com/58278652/185815101-68ed5797-fff8-4968-84e2-e6a47bba58d0.png)

- With partition filtering:

Here, we filter by the partitioned columns `year` and `state_code`. As a result, Bigquery only **queries** and **returns** the values from the **year** folder and the **state_code** subfolder.

![image](https://user-images.githubusercontent.com/58278652/185815135-fb012a2c-535b-457e-af2a-7984961168b3.png)

### **When should a table be partitioned?**

The first question that arises when dealing with partitioning is: _from which number of lines a table should be partitioned?_ The documentation of [GCP ](https://cloud.google.com/bigquery/docs/partitioned-tables?hl=pt-br) does not define a quantity _x_ or _y_  of lines that should be partitioned. The ideal is that tables are partitioned, with few exceptions. For example, tables with less than 10,000 lines, which will no longer receive data ingestion, do not have high storage and processing costs and, therefore, there is no need to be partitioned.

### **How to partition a table?**

If the data is stored locally, it is necessary:

1. Create the partitioned folders in your `/output` folder, using the language you are using.

Example of a partitioned table by `year` and `month`, using `python`:

```python
for year in [*range(2005, 2020)]:
  for month in [*range(1, 13)]:
    partition = output + f'table_id/year={year}/month={month}'
    if not os.path.exists(partition):
      os.makedirs(partition)
```
2. Save the partitioned files.

```python
for year in [*range(2005, 2020)]:
  for month in [*range(1, 13)]:
    df_partition = df[df['year'] == year].copy() # The .copy is not necessary, it's just a good practice
    df_partition = df_partition[df_partition['month'] == month]
    df_partition.drop(['year', 'month'], axis=1, inplace=True) # It's necessary to exclude the columns used for partitioning
    partition = output + f'table_id/year={year}/month={month}/table.csv'
    df_partition.to_csv(partition, index=False, encoding='utf-8', na_rep='')
```

Examples of partitioned tables in `R`:

- [PNADC](https://github.com/basedosdados/mais/blob/master/bases/br_ibge_pnadc/code/microdados.R)
- [PAM](https://github.com/basedosdados/mais/blob/master/bases/br_ibge_pam/code/permanentes_usando_api.R)

Example of how to partition a table in `SQL`:

```sql
CREATE TABLE `dataset_id.table_id` as (
    year  INT64,
    month  INT64,
    col1 STRING,
    col1 STRING
) PARTITION BY year, month
OPTIONS (Description='Description of the table')
```

### **Important rules for partitioning.**

- The types of columns that BigQuery accepts as partitioning are:
    * **Time unit column**: tables are partitioned based on a `TIMESTAMP`, `DATE` or `DATETIME` column.
    * **Processing time**: tables are partitioned based on the `data/time` stamp when BigQuery processes the data.
    * **Range of integers**: tables are partitioned based on a column of integers.

- The types of columns that BigQuery **does not** accept as partitioning are: `BOOL`, `FLOAT64`, `BYTES`, etc.

- BigQuery accepts up to 4,000 partitions per table.

- In our BD, tables are usually partitioned by: `year`, `month`, `quarter`, and `state_code`.

- Note that when partitioning a table, it is necessary to exclude the corresponding column. Example: it is necessary to exclude the `year` column when partitioning by `year`.


## Number of bases per _pull request_

Pull requests on Github should include a maximum of one dataset, but can include more than one base. In other words, they can involve one or more tables within the same dataset.

## Dictionaries

- Each base includes only one dictionary (which covers one or more tables).
- For each table, column, and temporal coverage, each key maps uniquely to a value.
- Keys cannot have null values.
- Dictionaries must cover all available keys in the original tables.
- Keys can only have zeros to the left when the variable's number of digits has meaning. When the variable is `enum` default, we exclude the zeros to the left.
    - Example: we keep the zero to the left of the variable `br_bd_diretorios_brasil.cbo_2002:cbo_2002`, which has six digits, because the first digit `0` means the category is from the `grand group = "Members of the armed forces, police, and firefighters"`.
    - For other cases, such as `br_inep_censo_escolar.stage:stage_education`, we exclude the zeros to the left. In other words, we change `01` to `1`.
- Values are standardized: without extra spaces, initial uppercase and rest lowercase, etc.

### **How to fill in the table dictionary metadata?**
- Do not fill in the *`spatial_coverage`* (`spatial_coverage`), i.e., leave the field empty.
- Do not fill in the *`temporal_coverage`* (`temporal_coverage`), i.e., leave the field empty.
- Do not fill in the *`observation_level`* (`observation_level`), i.e., leave the field empty.

## Directories

Directories are the fundamental building blocks of our _datalake_. Our rules for managing directories are:

- Directories represent _entities_ of the repository that have primary keys (e.g., `state`, `municipality`, `school`) and time-based units (e.g., `data`, `time`, `day`, `month`, `year`).
- Each directory table has at least one primary key with unique values and no nulls. Examples: `municipality:municipality_id`, `state:state_code`.
- Variable names with the `id_` prefix are reserved for primary keys of entities.

See all the [tables already available here.](https://basedosdados.org/dataset?organization=br-bd&order_by=score&q=%22directories%22)

### **How to fill in the directory table metadata?**
- Fill in the *`spatial_coverage`* (`spatial_coverage`), which is the maximum spatial unit that the table covers. Example: sa.br, which means that the spatial aggregation level of the table is Brazil.
- Do not fill in the *`temporal_coverage`* (`temporal_coverage`), i.e., leave the field empty.
- Fill in the *`observation_level`* (`observation_level`), which consists of the observation level of the table, i.e., what each line represents.
- Do not fill in the *`temporal_coverage`* (`temporal_coverage`) of the columns of the table, i.e., leave the field empty.

## Raw Data Sources

The field refers to the data in the raw data source, which has not yet passed through the Data Basis methodology, i.e., our `_input_`. When you click on it, the idea is to redirect the user to the original data source page. Our rules for managing the raw data sources are:

- Include the name of the external link that leads to the raw data source. As a default, this name should be the organization's name or the portal's name that stores the data. Examples: `Educational Statistics: Open Data from Inep`, `Penn World Tables: Groningen Growth and Development Centre`.
- Fill in the raw data source metadata: Description, URL, Language, Has Structured Data, Has an API, Is Free, Requires Registration, Availability, Requires IP from Some Country, License Type, Temporal Coverage, Spatial Coverage, and Observation Level.

## **Thought of improvements for the standards defined?**

Open an [issue on our Github](https://github.com/basedosdados/mais/labels/docs) or send a message on [Discord](https://discord.gg/huKWpsVYx4) to talk to us :)
