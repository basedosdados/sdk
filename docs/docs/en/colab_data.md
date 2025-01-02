# Upload data to DB

## Why should my organization upload data to DB?

- **Ability to cross-reference your databases with data from different
  organizations** in a simple and easy way. There are already hundreds of
  public datasets from the largest organizations in Brazil and worldwide present
  in our *datalake*.

- **Commitment to transparency, data quality, and
  development of better research, analysis, and solutions** for
  society. We not only democratize access to open data but also quality data.
  We have a specialized team that reviews and ensures the quality of
  data added to the *datalake*.

- **Participation in an ever-growing community**: thousands
  of journalists, researchers, developers already use and
  follow Data Basis.
  <!-- TODO: Add the link to our audience dashboard when it's ready :) -->

## Step by step to upload data

Want to upload data to DB and help us build this repository?
*Wonderful!* We've organized everything you need in the manual below in 8 steps

To facilitate the explanation, we'll follow a ready-made example with data from [RAIS](https://basedosdados.org/en/dataset/3e7c4d58-96ba-448e-b053-d385a829ef00).

!!! Tip "You can navigate through the steps in the menu on the left."
    We strongly suggest joining our [Discord
    channel](https://discord.gg/huKWpsVYx4) to ask questions and
    interact with the team and other contributors! 😉

### Before starting

Some knowledge is necessary to carry out this process:

- **Python, R, SQL and/or Stata**: to create data capture and cleaning codes.
- **Command line**: to set up your local environment
  and connection with Google Cloud.
- **Github**: to upload your code for review by
  our team.

!!! Tip "Don't have some of these skills but want to contribute?"
    We have a data team that can help you, just join [our
    Discord](https://discord.gg/huKWpsVYx4) and send a message in #want-to-contribute.

### How does the process work?

1. [Choose the dataset and understand more about the data](#1-choose-the-dataset-and-understand-more-about-the-data) - first we need to know what we're dealing with.
2. [Download our template folder](#2-download-our-template-folder) - it's time to structure the work to be done
3. [Fill in the architecture tables](#3-fill-in-the-architecture-tables) - it's essential to define the data structure before we start treatment
4. [Write data capture and cleaning code](#4-write-data-capture-and-cleaning-code) - time to get to work!
5. [(If necessary) Organize auxiliary files](#5-if-necessary-organize-auxiliary-files) - because even data needs guides
6. [(If necessary) Create dictionary table](#6-if-necessary-create-dictionary-table) - time to build the dictionaries
7. [Upload everything to Google Cloud](#7-upload-everything-to-google-cloud) - after all, that's where DB data is stored
8. [Send everything for review](#8-send-everything-for-review) - a look from our team to ensure everything is ready for production!


### 1. Choose the dataset and understand more about the data

We keep the list of datasets for volunteers in our [Github](https://github.com/orgs/basedosdados/projects/17/views/10). To start uploading a base of your interest, just open a [new issue](https://github.com/basedosdados/queries-basedosdados/issues/new?assignees=&labels=&projects=&template=issue--novos-dados.md&title=%5Bdados%5D+%3Cdataset_id%3E) with data. If your base (dataset) is already listed, just mark your Github user as `assignee`

Your first task is to fill in the information in the issue. These information will help you understand the data better and will be very useful for treatment and filling in metadata.

When you finish this step, call someone from the data team to that the information you mapped about the dataset already enter our site!
### 2. Download our template folder

[Download here the template
_template_](https://drive.google.com/drive/folders/1xXXon0vdjSKr8RCNcymRdOKgq64iqfS5?usp=sharing)
 and rename it to `<dataset_id>` (defined in the issue of [step 1](#-1-Choose-the-database-and-understand-more-about-the-data)). This template folder simplifies and organizes all the steps from here on. Its
structure is as follows:

- `<dataset_id>/`
    - `code/`: Necessary codes for **capture** and **cleaning** of data
    ([we'll see more in step
    4](#4-write-data-capture-and-cleaning-code)).
    - `input/`: Contains all the original files with data, exactly
    as downloaded from the primary source. ([we'll see more in step
    4](#4-write-data-capture-and-cleaning-code)).
    - `output/`: Final files, already in the ready-to-upload format ([we'll see more in step
    4](#4-write-data-capture-and-cleaning-code)).
    - `tmp/`: Any temporary files created by the code in `/code` during the cleaning and treatment process ([we'll see more in step
    4](#4-write-data-capture-and-cleaning-code)).
    - `extra/`
        - `architecture/`: Architecture tables ([we'll see more in step 3](#3-fill-in-the-architecture-tables)).
        - `auxiliary_files/`: Auxiliary files to the data ([we'll see more in step 5](#5-if-necessary-organize-auxiliary-files)).
        - `dicionario.csv`: Dictionary table for the entire dataset ([we'll see more in step 6](#6-if-necessary-create-dictionary-table)).

!!! info "Only the `code` folder will be committed to your project, the other files will only exist locally or in Google Cloud."

### 3. Fill in the architecture tables

The architecture tables determine **what the structure of
each table in your dataset is**. They define, for example, the name, order, and metadata of the variables, as well as compatibilities when there are changes in versions (for example, if a variable changes name from one year to the next).

!!! Info "Each dataset table must have its own architecture table (spreadsheet), which must be filled in **Google Drive** to allow correction by our data team."


#### Example: RAIS - Architecture tables

The RAIS architecture tables [can be consulted here](https://docs.google.com/spreadsheets/d/1dPLUCeE4MSjs0ykYUDsFd-e7-9Nk6LVV/edit?usp=sharing&ouid=103008455637924805982&rtpof=true&sd=true). They are a great reference for you to start your work since they have many variables and examples of various situations you might end up encountering.

#### To fill in each table of your dataset, follow these steps:

!!! Tip "A each beginning and end of step, consult our [style guide](style_data.md) to ensure you're following the BD standardization"

1. List all the variables in the data in the `original_name` column
    - Obs: If the base changes the name of the variables over time (like RAIS), it's necessary to make compatibilities between years for all the variables by filling in the `original_name_YYYY` column for each year or month available
2. Rename the variables according to our [manual](style_data.md) in the `name` column
3. Understand the type of variable and fill in the `bigquery_type` column
4. Fill in the description in the `description` column according to the [manual](style_data.md)
5. From the compatibilities between years and/or queries to the raw data, fill in the temporal coverage in `temporal_coverage` for each variable
    - Obs: If the variables have the same temporal coverage as the table, fill in only with '(1)'
6. Indicate with 'yes' or 'no' if there's a dictionary for the variables in `covered_by_dictionary`
7. Check if the variables represent any entity present in the [directories](https://basedosdados.org/dataset?q=diret%C3%B3rio&datasets_with=open_tables&organization=bd&page=1) to fill in the `directory_column`
8. For variables of type `int64` or `float64`, check if it's necessary to include a [measurement unit](https://github.com/basedosdados/website/blob/master/ckanext-basedosdados/ckanext/basedosdados/validator/available_options/measurement_unit.py)
9. Reorder the variables according to the [manual](style_data.md)

!!! Tip "When you finish filling in the architecture tables, contact the Data Basis team to validate everything. It's necessary that it's clear what the final format of the data should be _before_ starting to write the code. This way we avoid redoing the work."

### 4. Write data capture and cleaning code

After validating the architecture tables, we can write the **capture** and **cleaning** codes for the data.

- **Capture**: Code that automatically downloads all the original data and saves it in `/input`. These data can be available on portals or links FTP, can be scraped from websites, among others.

- **Cleaning**: Code that transforms the original data saved in `/input` into clean data, saves it in the `/output` folder, to be later uploaded to DB.

Each clean table for production can be saved as a single file or, if it's very large (e.g. above 200 mb), it can be partitioned in the [Hive](https://cloud.google.com/bigquery/docs/hive-partitioned-loads-gcs) format in several sub-files. The accepted formats are `.csv` or `.parquet`. Our recommendation is to partition tables by `year`, `month`, and `state_abbreviation`. The partitioning is done through the folder structure, see the example below to visualize how.

#### Example: RAIS - Partitioning
The `microdados_vinculos` table from RAIS, for example, is a very large table (+250GB) so we partition it by `year` and `state_abbreviation`. The partitioning was done using the folder structure `/microdados_vinculos/year=YYYY/state_abbreviation=XX` .

#### Required patterns in the code

- Must be written in [Python](https://www.python.org/),
  [R](https://www.r-project.org/) or [Stata](https://www.stata.com/) -
  so that the review can be performed by our team.
- Can be in script (`.py`, `.R`, ...) or *notebooks* (Google Colab, Jupyter, Rmarkdown, etc).
- File paths must be shortcuts _relative_ to the root folder
  (`<dataset_id>`), that is, they must not depend on the paths of your
  computer.
- The cleaning must follow our [style guide](style_data.md) and the [best programming practices](https://en.wikipedia.org/wiki/Best_coding_practices).

#### Example: PNAD Continuous - Cleaning Code

The cleaning code was built in R and [can be consulted
here](https://github.com/basedosdados/mais/tree/master/bases/br_ibge_pnadc/code).

#### Example: Activity in the Legislative Chamber - Download and Cleaning Code
The cleaning code was built in Python [can be consulted here](https://github.com/basedosdados/mais/tree/bea9a323afcea8aa1609e9ade2502ca91f88054c/bases/br_camara_atividade_legislativa/code)

### 5. (If necessary) Organize auxiliary files

It's common for databases to be made available with auxiliary files. These can include technical notes, collection and sampling descriptions, etc. To help users of Data Basis have more context and understand the data better, organize all these auxiliary files in `/extra/auxiliary_files`.

Feel free to structure sub-folders as you like there. What matters is that it's clear what these files are.

### 6. (If necessary) Create dictionary table

Sometimes, especially with old bases, there are multiple dictionaries in Excel or other formats. In Data Basis, we unify everything in a single file in `.csv` format - a single dictionary for all columns of all tables in your dataset.

!!! Info "Important details on how to build your dictionary are in our [style guide](../style_data/#dictionaries)."

#### Example: RAIS - Dictionary

The complete dictionary [can be consulted
here](https://docs.google.com/spreadsheets/d/12Wwp48ZJVux26rCotx43lzdWmVL54JinsNnLIV3jnyM/edit?usp=sharing).
It already has the standard structure we use for dictionaries.

### 7. Upload everything to Google Cloud

All set! Now all that's left is to upload to Google Cloud and send for review. For that, we'll use the `basedosdados` client (available in Python) that facilitates the process.

!!! Info "Since there's a cost for storage in the storage, to finalize this step we'll need to make you available an api_key specifically for volunteers to upload the data to our development environment. So, join our [Discord channel](https://discord.gg/huKWpsVYx4) and call us in 'want-to-contribute'"

#### Configure your credentials locally

**7.1** Install our client in your terminal: `pip install basedosdados`.

**7.2** Run `import basedosdados as bd` in python and follow the step-by-step process to configure locally with the credentials of your project in Google Cloud. Fill in the information as follows:
```
    * STEP 1: y
    * STEP 2: basedosdados-dev  (put the .json passed by the bd team in the credentials folder)
    * STEP 3: y
    * STEP 4: basedosdados-dev
    * STEP 5: https://api.basedosdados.org/api/v1/graphql
```
#### Upload the files to the Cloud
The data will pass through 3 places in Google Cloud:

  * **Storage**: also called GCS is the local where the "cold" files (architectures, data, auxiliary files) will be stored.
  * **BigQuery-DEV-Staging**: table that connects the data from storage to the basedosdados-dev project in bigquery
  * **BigQuery-DEV-Production**: table used for testing and treatment via SQL of the dataset

**7.3** Create the table in the *GCS bucket* and *BigQuery-DEV-staging*, using the Python API, as follows:

    ```python
    import basedosdados as bd

    tb = bd.Table(
      dataset_id='<dataset_id>',
      table_id='<table_id>')

    tb.create(
        path='<path_to_the_data>',
        if_table_exists='raise',
        if_storage_data_exists='raise',
    )
    ```

    The following parameters can be used:


    - `path` (required): the complete path of the file on your computer, like: `/Users/<your_username>/projects/basedosdados/mais/bases/[DATASET_ID]/output/microdados.csv`.


    !!! Tip "If your data is partitioned, the path must point to the folder where the partitions are. Otherwise, it must point to a `.csv` file (for example, microdados.csv)."

    - `force_dataset`: command that creates the dataset configuration files in BigQuery.
        - _True_: the dataset configuration files will be created in your project and, if it doesn't exist in BigQuery, it will be created automatically. **If you already created and configured the dataset, don't use this option, as it will overwrite files**.
        - _False_: the dataset won't be recreated and, if it doesn't exist, it will be created automatically.
    - `if_table_exists` : command used if the **table already exists in BQ**:
        - _raise_: returns an error message.
        - _replace_: replaces the table.
        - _pass_: does nothing.

    - `if_storage_data_exists`: command used if the **data already exists in Google Cloud Storage**:
        - _raise_: returns an error message
        - _replace_: replaces the existing data.
        - _pass_: does nothing.

    !!! Info "If the project doesn't exist in BigQuery, it will be automatically created"

  Consult our [API](../api_reference_cli) for more details on each method.

**7.4** Create the .sql and schema.yml files from the architecture table following this [documentation](https://github.com/basedosdados/pipelines/wiki/Fun%C3%A7%C3%A3o-%60create_yaml_file()%60)
!!! Tip "If you need, at this moment you can change the SQL query to perform final treatments from the `staging` table, you can include columns, remove columns, perform algebraic operations, substitute strings, etc. SQL is the limit!"

**7.5** Run and test the models locally following this [documentation](https://github.com/basedosdados/pipelines/wiki/Testar-modelos-dbt-localmente)

**7.6** Upload the table metadata to the site:
!!! Info "For now, only the data team has permissions to upload the table metadata to the site, so it will be necessary to contact us. We're already working to make it possible for volunteers to update data on the site in the near future."

**7.7** Upload the auxiliary files:
    ```python
    st = bd.Storage(
      dataset_id = <dataset_id>,
      table_id = <table_id>)

    st.upload(
      path='caminho_para_os_auxiliary_files',
      mode = 'auxiliary_files',
      if_exists = 'raise')
    ```

### 8. Send everything for review

Yay, that's it! Now all that's left is to send everything for review in
[the repository](https://github.com/basedosdados/queries-basedosdados) of Data Basis.

1. Clone our [repository](https://github.com/basedosdados/queries-basedosdados) locally.
2. Give a `cd` to the local folder of the repository and open a new branch with `git checkout -b [dataset_id]`. All additions and modifications will be included in this _branch_.
3. For each new table, include the file with name `table_id.sql` in the `queries-basedosdados/models/dataset_id/` folder by copying the queries you developed in step 7.
4. Include the schema.yaml file developed in step 7
5. If it's a new dataset, include the dataset according to the instructions in the `queries-basedosdados/dbt_project.yaml` file (don't forget to follow the alphabetical order to not mess up the organization)
6. Include your data capture and cleaning code in the `queries-basedosdados/models/dataset_id/code` folder
7. Now it's just about publishing the branch, opening a PR with the labels 'table-approve' and marking the data team for correction

**And now?** Our team will review the data and metadata submitted via Github. We can contact you to ask questions or request changes to the code. When everything is OK, we'll do a _merge_ of your *pull request* and the data will be automatically published on our platform!
