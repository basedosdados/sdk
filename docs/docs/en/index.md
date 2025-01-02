# Hello, world!

Data Basis' mission is to universalize the use of quality data
worldwide. For this, we created a tool that allows you to **access
important resources from various public datasets**, such as:

- **Processed Tables**: Complete tables, already processed and ready
  for analysis, available in our public datalake.

- **Original Data**: Links with useful information to explore more
  about the dataset, such as the original source and others.

!!! Info "We have a Data team and volunteers from all over Brazil and abroad who help clean and maintain processed tables. [Learn how to be part of it](colab_data)."

## Accessing DB processed tables

On our website you'll find the list of all processed tables for
each dataset. We also present important information about all
tables, such as the list of columns, temporal coverage, periodicity, among
other information. You can query the table data via:

### Download

You can download the complete CSV file of the table directly from the website. This
type of query is not available for files exceeding 200 thousand rows.

### BigQuery (SQL)

BigQuery is Google's cloud database service.
Directly from your browser, you can query the processed
tables with:

- Speed: Even very long queries take only minutes to process.

- Scale: BigQuery magically scales to hexabytes if needed.

- Economy: Every user has *1 TB free per month for querying
  the data*.

<a
href="access_data_bq"
title="{{ lang.t('source.link.title')}}" class="md-button"
hover="background-color: var(--md-primary-fg-color--dark)">
    Learn More
    :material-arrow-right:
</a>

### Packages

Base dos Dados packages allow access to the public *data lake*
directly from your computer or development environment.
<!-- Another way to access BD resources is directly through the endpoints, as
documented in [BD Open API](https://basedosdados.org/openapi).
 -->
The currently available packages are:

- **:material-language-python: Python**
- **:material-language-r: R**
- **Stata**

<a
href="access_data_packages"
title="{{ lang.t('source.link.title')}}" class="md-button"
hover="background-color: var(--md-primary-fg-color--dark)">
    Learn More
    :material-arrow-right:
</a>

## Tips for better data usage

Our data team constantly works on developing **better
standards and methodologies** to facilitate the data analysis process.
We've separated some useful materials for you to better understand what we do
and how to make the best use of the data:

- [Join tables from different organizations quickly](tutorial_join_tables)
- [Understand patterns of tables, datasets and variables](style_data)
