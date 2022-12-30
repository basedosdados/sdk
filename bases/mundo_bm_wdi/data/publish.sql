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

CREATE VIEW basedosdados-dev.mundo_bm_wdi.data AS
SELECT 
SAFE_CAST(country_id AS STRING) country_id,
SAFE_CAST(indicator_id AS STRING) indicator_id,
SAFE_CAST(year AS INT64) year,
SAFE_CAST(value AS FLOAT64) value
FROM basedosdados-dev.mundo_bm_wdi_staging.data AS t