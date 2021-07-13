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
CREATE VIEW `basedosdados-dev.br_inep_ideb.brasil_ano_escolar` AS
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(rede AS STRING) rede,
SAFE_CAST(ensino AS STRING) ensino,
SAFE_CAST(ano_escolar AS STRING) ano_escolar,
SAFE_CAST(taxa_aprovacao AS FLOAT64) taxa_aprovacao
FROM basedosdados-dev.br_inep_ideb_staging.brasil_ano_escolar AS t