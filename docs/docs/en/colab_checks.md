# Collaborating with tests on DB

To maintain the quality of databases present in DB, we rely on a set of automatic checks that are performed during the insertion and update of each database. These checks are necessary but not sufficient to ensure data quality. They perform basic queries, such as whether the table exists or if it has completely null columns.

You can collaborate with DB by increasing test coverage, thus reducing data review work. To do this, simply create SQL queries that test data quality, such as:

- Verify if columns with proportions have values between 0 and 100
- Verify if date columns follow the YYYY-MM-DD HH:MM:SS pattern

<!----------------------------------------------------------------------------->

## What's the procedure?

Including data tests should follow this workflow:

- [Collaborating with tests on BD+](#collaborating-with-tests-on-bd)
  - [What's the procedure?](#whats-the-procedure)
  - [1. Express your interest](#1-express-your-interest)
  - [2. Write your query](#2-write-your-query)
  - [3. Submit your query](#3-submit-your-query)

!!! Tip "We suggest joining our [Discord channel](https://discord.gg/huKWpsVYx4) to ask questions and interact with other contributors! :)"

<!----------------------------------------------------------------------------->

## 1. Express your interest

Chat with us in the infra chat or Monday meetings at 7 PM BRT, both on Discord. If you don't have an improvement suggestion, we can look for a query that hasn't been written yet.

<!----------------------------------------------------------------------------->

## 2. Write your query

Fork the [Data Basis](https://github.com/basedosdados/mais/tree/master) repository. Then add new queries and their respective execution functions in the files [checks.yaml](https://github.com/basedosdados/mais/blob/master/.github/workflows/data-check/checks.yaml) and [test_data.py](https://github.com/basedosdados/mais/blob/master/.github/workflows/data-check/test_data.py).

Queries are written in a YAML file with `Jinja` and SQL, in the following way:

```yaml
test_select_all_works:
  name: Check if select query in {{ table_id }} works
  query: |
    SELECT NOT EXISTS (
            SELECT *
        FROM `{{ project_id_staging }}.{{ dataset_id }}.{{ table_id }}`
    ) AS failure
```

And executed as `pytest` package tests:

```python
def test_select_all_works(configs):
    result = fetch_data("test_select_all_works", configs)
    assert result.failure.values == False
```

Don't worry if you're not familiar with some of the syntax above; we can help you during the process. Note that the values between curly braces are variables contained in `table_config.yaml` files, which contain table metadata. Therefore, query writing is limited by existing metadata. We recommend consulting these files in the [bases](https://github.com/basedosdados/mais/tree/master/bases) directory.

<!----------------------------------------------------------------------------->

## 3. Submit your query

Finally, make a pull request to the main repository for the query to be reviewed.
