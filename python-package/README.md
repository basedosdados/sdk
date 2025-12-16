# basedosdados

**basedosdados** is a Python package that provides easy access to Brazil’s largest open data lake, hosted on Google BigQuery. It allows you to query, download, and analyze high-quality, ready-to-use public datasets with minimal setup.

## Installation

Install from PyPI:

```bash
pip install basedosdados
```

Or using [uv](https://github.com/astral-sh/uv):

```bash
uv add basedosdados
```

Or with [poetry](https://python-poetry.org/):

```bash
poetry add basedosdados
```

## Quickstart

### Access a table

```python
import basedosdados as bd

df = bd.read_table('br_ibge_pib', 'municipio', billing_project_id="<YOUR-PROJECT>")
```

> On first use, you will be prompted to authenticate your Google Cloud project. [See how to create a project here.](https://basedosdados.org/docs/access_data_bq#bigquery)

### Run a SQL query

```python
import basedosdados as bd

query = """
SELECT *
FROM `basedosdados.br_tse_eleicoes.bens_candidato`
WHERE ano = 2020
AND sigla_uf = 'TO'
"""

df = bd.read_sql(query, billing_project_id="<YOUR-PROJECT>")
```

### Set global configuration

You can set your billing project globally to avoid passing it every time:

```python
import basedosdados as bd

bd.config.billing_project_id = "<YOUR-PROJECT-ID>"

query = """
SELECT *
FROM `basedosdados.br_bd_diretorios_brasil.municipio`
"""

df = bd.read_sql(query=query)
```

### Download query results to CSV

```python
import basedosdados as bd

query = """
SELECT ano, id_municipio, pib
FROM `basedosdados.br_ibge_pib.municipio`
WHERE ano = 2010
"""

bd.download("pib_2010.csv", query=query, billing_project_id="<YOUR-PROJECT>")
```

## Documentation

- [API Reference](https://basedosdados.org/docs/api_reference_python)

## Advanced: Multiple Configurations

If you need to use multiple service accounts or configurations, you can manage them by renaming the config folder and setting `bd.config.project_config_path`:

```python
import basedosdados as bd
bd.config.project_config_path = "/home/user/.bd_my_other_account"
```

## Contributing

See our [contribution guide](CONTRIBUTING.md) for how to help improve this package.

## License

MIT License. See [LICENSE](LICENSE) for details.

---

> Part of the [Base dos Dados](https://basedosdados.org) project – universalizing access to quality data in Brazil.
