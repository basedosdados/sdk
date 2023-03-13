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

CREATE VIEW basedosdados-dev.br_ibge_pof.rendimento_trabalho_2017 AS
SELECT 
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(situacao AS STRING) situacao,
SAFE_CAST(id_estrato AS STRING) id_estrato,
SAFE_CAST(id_unidade_primaria_amostragem AS STRING) id_unidade_primaria_amostragem,
SAFE_CAST(id_domicilio AS STRING) id_domicilio,
SAFE_CAST(id_unidade_consumo AS STRING) id_unidade_consumo,
SAFE_CAST(id_informante AS STRING) id_informante,
SAFE_CAST(id_quadro AS STRING) id_quadro,
SAFE_CAST(id_sub_quadro AS STRING) id_sub_quadro,
SAFE_CAST(id_codigo_5_bd AS STRING) id_codigo_5_bd,
SAFE_CAST(V9001 AS STRING) V9001,
SAFE_CAST(V5302 AS STRING) V5302,
SAFE_CAST(V53021 AS STRING) V53021,
SAFE_CAST(V5303 AS STRING) V5303,
SAFE_CAST(V5304 AS STRING) V5304,
SAFE_CAST(V5305 AS STRING) V5305,
SAFE_CAST(V5307 AS STRING) V5307,
SAFE_CAST(V5314 AS INT64) V5314,
SAFE_CAST(V5315 AS STRING) V5315,
SAFE_CAST(V9010 AS STRING) V9010,
SAFE_CAST(V9011 AS INT64) V9011,
SAFE_CAST(V53011 AS STRING) V53011,
SAFE_CAST(V53061 AS STRING) V53061,
SAFE_CAST(indicador_imputacao_valor AS INT64) indicador_imputacao_valor,
SAFE_CAST(fator_anualizacao AS INT64) fator_anualizacao,
SAFE_CAST(deflator AS FLOAT64) deflator,
SAFE_CAST(V8500 AS FLOAT64) V8500,
SAFE_CAST(V531112 AS FLOAT64) V531112,
SAFE_CAST(V531122 AS FLOAT64) V531122,
SAFE_CAST(V531132 AS FLOAT64) V531132,
SAFE_CAST(V8500_deflacionado AS FLOAT64) V8500_deflacionado,
SAFE_CAST(V531112_deflacionado AS FLOAT64) V531112_deflacionado,
SAFE_CAST(V531122_deflacionado AS FLOAT64) V531122_deflacionado,
SAFE_CAST(V531132_deflacionado AS FLOAT64) V531132_deflacionado,
SAFE_CAST(renda_total AS FLOAT64) renda_total,
SAFE_CAST(peso AS FLOAT64) peso,
SAFE_CAST(peso_final AS FLOAT64) peso_final
FROM basedosdados-dev.br_ibge_pof_staging.rendimento_trabalho_2017 AS t