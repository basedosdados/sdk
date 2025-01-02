# How to join tables in the **data lake**

We organize the data so that joining tables from different institutions and themes
is as simple as any other query. For this, we defined a standard methodology for
data treatment, column naming, tables, and datasets.

??? Info "How does the DB methodology work?"

Information from different tables can be aggregated through **identifier keys**.
An identifier key is a column whose name is unique across all tables in the
*data lake* and is used to identify an entity.

## Example of an identifier key

The `year` column has the same name in all *data lake* tables - it always refers
to the variable that has any years from our calendar as its value.

When working with IBGE population data, the `year` column, along with the
`municipality` column, uniquely identify each row in the table:

- **There is no more than one row with the same year and municipality;**

- **There is no row with null values for `year` or `municipality` in the table;**

!!! Tip "Test it yourself: the queries below should return empty!"

=== "R"
    ```R
    library("basedosdados")

    # Search for any row that has repeated year and municipality
    query <- "SELECT ano, municipio, count(*) as total
    FROM `basedosdados.br_ibge_populacao.municipio`
    GROUP BY ano, municipio
    WHERE total > 1"
    read_sql(query=query)

    # Search for rows with null year or municipality
    query <- "SELECT * FROM
    `basedosdados.br_ibge_populacao.municipio`
    WHERE ano IS NULL OR municipio IS NULL"
    read_sql(query=query)
    ```

=== "Python"
    ```python
    import basedadosdados as bd

    # Search for any row that has repeated year and municipality
    query = """SELECT ano, municipio, count(*) as total
    FROM `basedosdados.br_ibge_populacao.municipio`
    GROUP BY ano, municipio
    WHERE total > 1"""
    bd.read_sql(query=query)

    # Search for rows with null year or municipality
    query = """SELECT * FROM
    `basedosdados.br_ibge_populacao.municipio`
    WHERE ano IS NULL OR municipio IS NULL"""
    bd.read_sql(query=query)
    ```

=== "CLI"
    ```bash
    ...
    ```

## Crossing tables with identifier keys

The indication of a set of columns as an identifier key is made directly in the
table metadata. Thus, you can know which tables can be crossed by comparing the
set of identifier keys of each one.

Below we'll make an example of how to cross IBGE's population and GDP tables to
obtain the GDP per capita of all Brazilian municipalities.

In the population and GDP tables, the `year` and `municipality` columns are
identifier keys. Therefore, we'll use these columns in our `JOIN` function to
determine how to cross the tables.

=== "R"
    ```R
    library("basedosdados")

    set_billing_id("<YOUR_PROJECT_ID>")

    query <- "SELECT
        pib.id_municipio,
        pop.ano,
        pib.PIB / pop.populacao as pib_per_capita
        FROM `basedosdados.br_ibge_pib.municipio` as pib
            JOIN `basedosdados.br_ibge_populacao.municipio` as pop
            ON pib.id_municipio = pop.id_municipio AND pib.ano = pop.ano"

    # You can download to your computer
    dir <- tempdir()
    data <- download(query, file.path(dir, "gdp_per_capita.csv"))

    # Or load the query result into your analysis environment
    data <- read_sql(query)
    ```

=== "Python"
    ```python
    import basedosdados as bd

    gdp_per_capita = """SELECT
        pib.id_municipio ,
        pop.ano,
        pib.PIB / pop.populacao as pib_per_capita
    FROM `basedosdados.br_ibge_pib.municipio` as pib
        INNER JOIN `basedosdados.br_ibge_populacao.municipio` as pop
        ON pib.id_municipio = pop.id_municipio AND pib.ano = pop.ano
    """

    # You can download to your computer
    bd.download(query=gdp_per_capita,
                savepath="where/to/save/file",
                billing_project_id=<YOUR_PROJECT_ID>)

    # Or load the query result into pandas
    df = bd.read_sql(gdp_per_capita, billing_project_id=<YOUR_PROJECT_ID>)
    ```

<!-- TODO: CROSSING EXAMPLE -->

## List of identifier keys

### Geographic keys

- Census tract: `id_setor_censitario`

- Municipality: `id_municipio` (standard), `id_municipio_6`, `id_municipio_tse`, `id_municipio_rf`, `id_municipio_bcb`

- Minimum Comparable Area: `id_AMC`

- Immediate region: `id_regiao_imediata`

- Intermediate region: `id_regiao_intermediaria`

- Microregion: `id_microrregiao`

- Mesoregion: `id_mesorregiao`

- Federal unit (State): `sigla_uf` (standard), `id_uf`, `uf`

- Region: `regiao`

### Time keys

- `ano` (year), `semestre` (semester), `mes` (month), `semana` (week), `dia` (day), `hora` (hour)

### Individual person keys

- `cpf`, `pis`, `nis`

### Legal entity keys

- Company: `cnpj`

- School: `id_escola`

### Political keys

- Candidate: `id_candidato_bd`

- Party: `sigla_partido`, `partido`
