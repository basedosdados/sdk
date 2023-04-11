/*
Query para publicar a tabela.

Esse é o lugar para:
    - modificar nomes, ordem e tipos de colunas
    - dar join com outras tabelas
    - criar colunas extras (e.g. logs, proporções, etc.)

Qualquer coluna definida aqui deve também existir em `table_config.yaml`.

# Além disso, sinta-se à vontade para alterar alguns nomes obscuros
# para algo um pouco mais explícito.

TIPOS:
    - Para modificar tipos de colunas, basta substituir STRING por outro tipo válido.
    - Exemplo: `SAFE_CAST(column_name AS NUMERIC) column_name`
    - Mais detalhes: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
*/

CREATE VIEW basedosdados-dev.br_bd_metadados.organizations AS
SELECT 
SAFE_CAST(id AS STRING) id,
SAFE_CAST(name AS STRING) name,
SAFE_CAST(description AS STRING) description,
SAFE_CAST(display_name AS STRING) display_name,
SAFE_CAST(title AS STRING) title,
SAFE_CAST(package_count AS INT64) package_count,
SAFE_CAST(date_created AS DATE) date_created,
FROM basedosdados-dev.br_bd_metadados_staging.organizations AS t