# Cómo unir tablas en el **datalake**

Organizamos los datos de manera que la unión de tablas de diferentes
instituciones y temas sea tan simple como cualquier otra consulta.
Para ello, definimos una metodología estándar para el tratamiento de datos,
nomenclatura de columnas, tablas y conjuntos.

??? Info "¿Cómo funciona la metodología BD?"
    Alguna frase sobre ....
    Para saber más, lea la documentación sobre tratamiento y arquitectura
    de datos.

La información de diferentes tablas se puede agregar mediante
**claves identificadoras**. Una clave identificadora es una columna cuyo nombre
es único en todas las tablas del *data lake* y se utiliza para
identificar una entidad.

## Ejemplo de clave identificadora

La columna `ano` tiene el mismo nombre en todas las tablas del *data lake* -
siempre se refiere a la variable que tiene como valor cualquier año de
nuestro calendario.

Cuando trabajamos con datos de población del IBGE, la columna `ano`,
junto con la columna `municipio`, identifican de manera única cada fila de
la tabla:

- **No existe más de una fila con el mismo año y municipio;**

- **No existe fila con valor nulo de `ano` o `municipio` en la tabla;**

!!! Tip "¡Pruébalo tú mismo(a): las siguientes consultas deben retornar vacío!"

=== "R"
    ```R
    library("basedosdados")

    # Busca alguna fila que tenga año y municipio repetido
    query <- "SELECT ano, municipio, count(*) as total
    FROM `basedosdados.br_ibge_populacao.municipios`
    GROUP BY ano, municipio
    WHERE total > 1"
    read_sql(query=query)

    # Busca filas con año o municipio nulos
    query <- "SELECT * FROM
    `basedosdados.br_ibge_populacao.municipios`
    WHERE ano IS NULL OR municipio IS NULL"
    read_sql(query=query)
    ```

=== "Python"
    ```python
    import basedadosdados as bd

    # Busca alguna fila que tenga año y municipio repetido
    query = """SELECT ano, municipio, count(*) as total
    FROM `basedosdados.br_ibge_populacao.municipios`
    GROUP BY ano, municipio
    WHERE total > 1"""
    bd.read_sql(query=query)

    # Busca filas con año o municipio nulos
    query = """SELECT * FROM
    `basedosdados.br_ibge_populacao.municipios`
    WHERE ano IS NULL OR municipio IS NULL"""
    bd.read_sql(query=query)
    ```

=== "CLI"
    ```bash
    ...
    ```

## Uniendo tablas con claves identificadoras

La indicación de un conjunto de columnas como clave identificadora se hace
directamente en los metadatos de la tabla. Así, puedes saber qué tablas
pueden unirse comparando el conjunto de claves identificadoras de
cada una.

A continuación, haremos un ejemplo de cómo unir las tablas de población y PIB del
IBGE para obtener el PIB per cápita de todos los municipios brasileños.

En las tablas de población y PIB, la columna `ano` y `municipio` son claves
identificadoras. Por lo tanto, usaremos estas columnas en nuestra función `JOIN` para
determinar cómo unir las tablas.

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

    # Puedes descargar en tu computadora
    dir <- tempdir()
    data <- download(query, file.path(dir, "pib_per_capita.csv"))

    # O cargar el resultado de la consulta en tu ambiente de análisis
    data <- read_sql(query)
    ```

=== "Python"
    ```python
    import basedadosdados as bd

    pib_per_capita = """SELECT
        pib.id_municipio ,
        pop.ano,
        pib.PIB / pop.populacao as pib_per_capita
    FROM `basedosdados.br_ibge_pib.municipio` as pib
        INNER JOIN `basedosdados.br_ibge_populacao.municipio` as pop
        ON pib.id_municipio = pop.id_municipio AND pib.ano = pop.ano
    """

    # Puedes descargar en tu computadora
    bd.download(query=pib_per_capita,
                savepath="where/to/save/file",
                billing_project_id=<YOUR_PROJECT_ID>)

    # O cargar el resultado de la consulta en pandas
    df = bd.read_sql(pib_per_capita, billing_project_id=<YOUR_PROJECT_ID>)
    ```

<!-- TODO: EJEMPLO DE UNIÓN -->

## Lista de claves identificadoras

### Claves geográficas

- Sector censal: `id_setor_censitario`

- Municipio: `id_municipio` (estándar), `id_municipio_6`, `id_municipio_tse`, `id_municipio_rf`, `id_municipio_bcb`

- Área Mínima Comparable: `id_AMC`

- Región inmediata: `id_regiao_imediata`

- Región intermediaria: `id_regiao_intermediaria`

- Microrregión: `id_microrregiao`

- Mesorregión: `id_mesorregiao`

- Unidad de la federación (UF): `sigla_uf` (estándar), `id_uf`, `uf`

- Región: `regiao`

### Claves temporales

- `ano`, `semestre`, `mes`, `semana`, `dia`, `hora`

### Claves de personas físicas

- `cpf`, `pis`, `nis`

### Claves de personas jurídicas

- Empresa: `cnpj`

- Escuela: `id_escola`

### Claves en política

- Candidato(a): `id_candidato_bd`

- Partido: `sigla_partido`, `partido`
