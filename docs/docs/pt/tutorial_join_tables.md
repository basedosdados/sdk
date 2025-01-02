# Como cruzar tabelas no **datalake**

Organizamos os dados de forma que o cruzamento de tabelas de diferentes
intituições e temas seja tão simples quanto qualquer outra consulta.
Para isso, definimos uma metodologia padrão para tratamento dos dados,
nomeação de colunas, tabelas e conjuntos.

??? Info "Como funciona a metolodiga BD?"
    Alguma frase sobre ....
    Para saber mais, leia a documentação sobre tratamento e arquitetura
    de dados.

Informações de diferentes tabelas podem ser agregadas por meiode
**chaves identificadora**. Uma chave identificadora é uma coluna cujo nome
é único em todas as tabelas do *data lake* e é utilizada para
identificar uma entidade.

## Exemplo de chave identificadora

A coluna `ano` tem esse mesmo nome em todas as tabelas do *data lake* -
ela sempre se refere a variável que tem como valor quaisquer anos do
nosso calendário.

Quando vamos trabalhar com dados de população do IBGE, a coluna `ano`,
junto com a coluna `municipio`, identificam unicamente cada linha da
tabela:

- **Não existe mais de uma linha com o mesmo ano e município;**

- **Não existe linha com valor nulo de `ano` ou `municipio` na tabela;**

!!! Tip "Teste você mesmo(a): as queries abaixo devem retornar vazio!"

=== "R"
    ```R
    library("basedosdados")

    # Busca alguma linha que possui ano e município repetido
    query <- "SELECT ano, municipio, count(*) as total
    FROM `basedosdados.br_ibge_populacao.municipios`
    GROUP BY ano, municipio
    WHERE total > 1"
    read_sql(query=query)

    # Busca linhas com ano ou municipio nulos
    query <- "SELECT * FROM
    `basedosdados.br_ibge_populacao.municipios`
    WHERE ano IS NULL OR municipio IS NULL"
    read_sql(query=query)
    ```

=== "Python"
    ```python
    import basedadosdados as bd

    # Busca alguma linha que possui ano e município repetido
    query = """SELECT ano, municipio, count(*) as total
    FROM `basedosdados.br_ibge_populacao.municipios`
    GROUP BY ano, municipio
    WHERE total > 1"""
    bd.read_sql(query=query)

    # Busca linhas com ano ou municipio nulos
    query = """SELECT * FROM
    `basedosdados.br_ibge_populacao.municipios`
    WHERE ano IS NULL OR municipio IS NULL"""
    bd.read_sql(query=query)
    ```

=== "CLI"
    ```bash
    ...
    ```

## Cruzando tabelas com chaves identificadoras

A indicação de um conjunto de colunas como chave identificadora é feita
direto nos metadados da tabela. Assim, você pode saber quais tabelas
podem ser cruzadas comparando o conjunto de chaves identificadoras de
cada uma.

Abaixo vamos fazer um exemplo de como cruzar as tabelas de população e PIB do
IBGE para obter o PIB per capita de todos os municípios brasileiros.

Nas tabelas de população e PIB, a coluna `ano` e `municipio ` são chaves
identificadoras. Logo usaremos essas colunas na nossa função `JOIN` para
determinar como cruzar as tabelas.

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

    # Você pode fazer o download no seu computador
    dir <- tempdir()
    data <- download(query, file.path(dir, "pib_per_capita.csv"))

    # Ou carregar o resultado da query no seu ambiente de análise
    data <- read_sql(query)
    ```

=== "Python"
    ```python
    import basedosdados as bd

    pib_per_capita = """SELECT
        pib.id_municipio ,
        pop.ano,
        pib.PIB / pop.populacao as pib_per_capita
    FROM `basedosdados.br_ibge_pib.municipio` as pib
        INNER JOIN `basedosdados.br_ibge_populacao.municipio` as pop
        ON pib.id_municipio = pop.id_municipio AND pib.ano = pop.ano
    """

    # Você pode fazer o download no seu computador
    bd.download(query=pib_per_capita,
                savepath="where/to/save/file",
                billing_project_id=<YOUR_PROJECT_ID>)

    # Ou carregar o resultado da query no pandas
    df = bd.read_sql(pib_per_capita, billing_project_id=<YOUR_PROJECT_ID>)
    ```

<!-- TODO: EXEMPLO DE CRUZAMENTO -->

## Lista de chaves identificadoras

### Chaves geográficas

- Setor censitário: `id_setor_censitario`

- Município: `id_municipio` (padrão), `id_municipio_6`, `id_municipio_tse`, `id_municipio_rf`, `id_municipio_bcb`

- Área Mínima Comparável: `id_AMC`

- Região imediata: `id_regiao_imediata`

- Região intermediária: `id_regiao_intermediaria`

- Microrregião: `id_microrregiao`

- Mesorregião: `id_mesorregiao`

- Unidade da federação (UF):  `sigla_uf` (padrão), `id_uf`, `uf`

- Região: `regiao`

### Chaves temporais

- `ano`, `semestre`, `mes`, `semana`, `dia`, `hora`

### Chaves de pessoas físicas

- `cpf`, `pis`, `nis`

### Chaves de pessoas jurídicas

- Empresa: `cnpj`

- Escola: `id_escola`

### Chaves em política

- Candidato(a): `id_candidato_bd`

- Partido: `sigla_partido`, `partido`
