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

CREATE VIEW basedosdados-312117.br_ibge_inflacao.ipca15 AS
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(indice AS FLOAT64) indice,
SAFE_CAST(var_mes AS FLOAT64) var_mes,
SAFE_CAST(var_tres_meses AS FLOAT64) var_tres_meses,
SAFE_CAST(var_semestral AS FLOAT64) var_semestral,
SAFE_CAST(var_anual AS FLOAT64) var_anual,
SAFE_CAST(var_doze_meses AS FLOAT64) var_doze_meses
from basedosdados-312117.br_ibge_inflacao_staging.ipca15 as t