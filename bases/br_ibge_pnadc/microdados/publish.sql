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
CREATE VIEW basedosdados-dev.br_ibge_pnadc.microdados AS
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(trimestre AS INT64) trimestre,
SAFE_CAST(id_uf AS STRING) id_uf,
SAFE_CAST(sigla_uf AS STRING) sigla_uf,
SAFE_CAST(capital AS STRING) capital,
SAFE_CAST(rm_ride AS STRING) rm_ride,
SAFE_CAST(id_upa AS STRING) id_upa,
SAFE_CAST(id_estrato AS STRING) id_estrato,
SAFE_CAST(v1008 AS INT64) v1008,
SAFE_CAST(v1014 AS INT64) v1014,
SAFE_CAST(v1016 AS INT64) v1016,
SAFE_CAST(v1022 AS INT64) v1022,
SAFE_CAST(v1023 AS INT64) v1023,
SAFE_CAST(v1027 AS FLOAT64) v1027,
SAFE_CAST(v1028 AS FLOAT64) v1028,
SAFE_CAST(v1029 AS INT64) v1029,
SAFE_CAST(posest AS INT64) posest,
SAFE_CAST(v2001 AS INT64) v2001,
SAFE_CAST(v2003 AS INT64) v2003,
SAFE_CAST(v2005 AS INT64) v2005,
SAFE_CAST(v2007 AS INT64) v2007,
SAFE_CAST(v2008 AS INT64) v2008,
SAFE_CAST(v20081 AS INT64) v20081,
SAFE_CAST(v20082 AS INT64) v20082,
SAFE_CAST(v2009 AS INT64) v2009,
SAFE_CAST(v2010 AS INT64) v2010,
SAFE_CAST(v3001 AS INT64) v3001,
SAFE_CAST(v3002 AS INT64) v3002,
SAFE_CAST(v3002a AS INT64) v3002a,
SAFE_CAST(v3003 AS INT64) v3003,
SAFE_CAST(v3003a AS INT64) v3003a,
SAFE_CAST(v3004 AS INT64) v3004,
SAFE_CAST(v3005 AS INT64) v3005,
SAFE_CAST(v3005a AS INT64) v3005a,
SAFE_CAST(v3006 AS INT64) v3006,
SAFE_CAST(v3006a AS INT64) v3006a,
SAFE_CAST(v3007 AS INT64) v3007,
SAFE_CAST(v3008 AS INT64) v3008,
SAFE_CAST(v3009 AS INT64) v3009,
SAFE_CAST(v3009a AS INT64) v3009a,
SAFE_CAST(v3010 AS INT64) v3010,
SAFE_CAST(v3011 AS INT64) v3011,
SAFE_CAST(v3011a AS INT64) v3011a,
SAFE_CAST(v3012 AS INT64) v3012,
SAFE_CAST(v3013 AS INT64) v3013,
SAFE_CAST(v3013a AS INT64) v3013a,
SAFE_CAST(v3013b AS INT64) v3013b,
SAFE_CAST(v3014 AS INT64) v3014,
SAFE_CAST(v4001 AS INT64) v4001,
SAFE_CAST(v4002 AS INT64) v4002,
SAFE_CAST(v4003 AS INT64) v4003,
SAFE_CAST(v4004 AS INT64) v4004,
SAFE_CAST(v4005 AS INT64) v4005,
SAFE_CAST(v4006 AS INT64) v4006,
SAFE_CAST(v4006a AS INT64) v4006a,
SAFE_CAST(v4007 AS INT64) v4007,
SAFE_CAST(v4008 AS INT64) v4008,
SAFE_CAST(v40081 AS INT64) v40081,
SAFE_CAST(v40082 AS INT64) v40082,
SAFE_CAST(v40083 AS INT64) v40083,
SAFE_CAST(v4009 AS INT64) v4009,
SAFE_CAST(v4010 AS STRING) v4010,
SAFE_CAST(v4012 AS INT64) v4012,
SAFE_CAST(v40121 AS INT64) v40121,
SAFE_CAST(v4013 AS STRING) v4013,
SAFE_CAST(v40132 AS INT64) v40132,
SAFE_CAST(v40132a AS INT64) v40132a,
SAFE_CAST(v4014 AS INT64) v4014,
SAFE_CAST(v4015 AS INT64) v4015,
SAFE_CAST(v40151 AS INT64) v40151,
SAFE_CAST(v401511 AS INT64) v401511,
SAFE_CAST(v401512 AS INT64) v401512,
SAFE_CAST(v4016 AS INT64) v4016,
SAFE_CAST(v40161 AS INT64) v40161,
SAFE_CAST(v40162 AS INT64) v40162,
SAFE_CAST(v40163 AS INT64) v40163,
SAFE_CAST(v4017 AS INT64) v4017,
SAFE_CAST(v40171 AS INT64) v40171,
SAFE_CAST(v401711 AS INT64) v401711,
SAFE_CAST(v4018 AS INT64) v4018,
SAFE_CAST(v40181 AS INT64) v40181,
SAFE_CAST(v40182 AS INT64) v40182,
SAFE_CAST(v40183 AS INT64) v40183,
SAFE_CAST(v4019 AS INT64) v4019,
SAFE_CAST(v4020 AS INT64) v4020,
SAFE_CAST(v4021 AS INT64) v4021,
SAFE_CAST(v4022 AS INT64) v4022,
SAFE_CAST(v4024 AS INT64) v4024,
SAFE_CAST(v4025 AS INT64) v4025,
SAFE_CAST(v4026 AS INT64) v4026,
SAFE_CAST(v4027 AS INT64) v4027,
SAFE_CAST(v4028 AS INT64) v4028,
SAFE_CAST(v4029 AS INT64) v4029,
SAFE_CAST(v4032 AS INT64) v4032,
SAFE_CAST(v4033 AS INT64) v4033,
SAFE_CAST(v40331 AS INT64) v40331,
SAFE_CAST(v403311 AS INT64) v403311,
SAFE_CAST(v403312 AS INT64) v403312,
SAFE_CAST(v40332 AS INT64) v40332,
SAFE_CAST(v403321 AS INT64) v403321,
SAFE_CAST(v403322 AS INT64) v403322,
SAFE_CAST(v40333 AS INT64) v40333,
SAFE_CAST(v403331 AS INT64) v403331,
SAFE_CAST(v4034 AS INT64) v4034,
SAFE_CAST(v40341 AS INT64) v40341,
SAFE_CAST(v403411 AS INT64) v403411,
SAFE_CAST(v403412 AS INT64) v403412,
SAFE_CAST(v40342 AS INT64) v40342,
SAFE_CAST(v403421 AS INT64) v403421,
SAFE_CAST(v403422 AS INT64) v403422,
SAFE_CAST(v4039 AS INT64) v4039,
SAFE_CAST(v4039c AS INT64) v4039c,
SAFE_CAST(v4040 AS INT64) v4040,
SAFE_CAST(v40401 AS INT64) v40401,
SAFE_CAST(v40402 AS INT64) v40402,
SAFE_CAST(v40403 AS INT64) v40403,
SAFE_CAST(v4041 AS STRING) v4041,
SAFE_CAST(v4043 AS INT64) v4043,
SAFE_CAST(v40431 AS INT64) v40431,
SAFE_CAST(v4044 AS STRING) v4044,
SAFE_CAST(v4045 AS INT64) v4045,
SAFE_CAST(v4046 AS INT64) v4046,
SAFE_CAST(v4047 AS INT64) v4047,
SAFE_CAST(v4048 AS INT64) v4048,
SAFE_CAST(v4049 AS INT64) v4049,
SAFE_CAST(v4050 AS INT64) v4050,
SAFE_CAST(v40501 AS INT64) v40501,
SAFE_CAST(v405011 AS INT64) v405011,
SAFE_CAST(v405012 AS INT64) v405012,
SAFE_CAST(v40502 AS INT64) v40502,
SAFE_CAST(v405021 AS INT64) v405021,
SAFE_CAST(v405022 AS INT64) v405022,
SAFE_CAST(v40503 AS INT64) v40503,
SAFE_CAST(v405031 AS INT64) v405031,
SAFE_CAST(v4051 AS INT64) v4051,
SAFE_CAST(v40511 AS INT64) v40511,
SAFE_CAST(v405111 AS INT64) v405111,
SAFE_CAST(v405112 AS INT64) v405112,
SAFE_CAST(v40512 AS INT64) v40512,
SAFE_CAST(v405121 AS INT64) v405121,
SAFE_CAST(v405122 AS INT64) v405122,
SAFE_CAST(v4056 AS INT64) v4056,
SAFE_CAST(v4056c AS INT64) v4056c,
SAFE_CAST(v4057 AS INT64) v4057,
SAFE_CAST(v4058 AS INT64) v4058,
SAFE_CAST(v40581 AS INT64) v40581,
SAFE_CAST(v405811 AS INT64) v405811,
SAFE_CAST(v405812 AS INT64) v405812,
SAFE_CAST(v40582 AS INT64) v40582,
SAFE_CAST(v405821 AS INT64) v405821,
SAFE_CAST(v405822 AS INT64) v405822,
SAFE_CAST(v40583 AS INT64) v40583,
SAFE_CAST(v405831 AS INT64) v405831,
SAFE_CAST(v40584 AS INT64) v40584,
SAFE_CAST(v4059 AS INT64) v4059,
SAFE_CAST(v40591 AS INT64) v40591,
SAFE_CAST(v405911 AS INT64) v405911,
SAFE_CAST(v405912 AS INT64) v405912,
SAFE_CAST(v40592 AS INT64) v40592,
SAFE_CAST(v405921 AS INT64) v405921,
SAFE_CAST(v405922 AS INT64) v405922,
SAFE_CAST(v4062 AS INT64) v4062,
SAFE_CAST(v4062c AS INT64) v4062c,
SAFE_CAST(v4063 AS INT64) v4063,
SAFE_CAST(v4063a AS INT64) v4063a,
SAFE_CAST(v4064 AS INT64) v4064,
SAFE_CAST(v4064a AS INT64) v4064a,
SAFE_CAST(v4071 AS INT64) v4071,
SAFE_CAST(v4072 AS INT64) v4072,
SAFE_CAST(v4072a AS INT64) v4072a,
SAFE_CAST(v4073 AS INT64) v4073,
SAFE_CAST(v4074 AS INT64) v4074,
SAFE_CAST(v4074a AS INT64) v4074a,
SAFE_CAST(v4075a AS INT64) v4075a,
SAFE_CAST(v4075a1 AS INT64) v4075a1,
SAFE_CAST(v4076 AS INT64) v4076,
SAFE_CAST(v40761 AS INT64) v40761,
SAFE_CAST(v40762 AS INT64) v40762,
SAFE_CAST(v40763 AS INT64) v40763,
SAFE_CAST(v4077 AS INT64) v4077,
SAFE_CAST(v4078 AS INT64) v4078,
SAFE_CAST(v4078a AS INT64) v4078a,
SAFE_CAST(v4082 AS INT64) v4082,
SAFE_CAST(vd2002 AS INT64) vd2002,
SAFE_CAST(vd2003 AS INT64) vd2003,
SAFE_CAST(vd2004 AS INT64) vd2004,
SAFE_CAST(vd3004 AS INT64) vd3004,
SAFE_CAST(vd3005 AS INT64) vd3005,
SAFE_CAST(vd3006 AS INT64) vd3006,
SAFE_CAST(vd4001 AS INT64) vd4001,
SAFE_CAST(vd4002 AS INT64) vd4002,
SAFE_CAST(vd4003 AS INT64) vd4003,
SAFE_CAST(vd4004 AS INT64) vd4004,
SAFE_CAST(vd4004a AS INT64) vd4004a,
SAFE_CAST(vd4005 AS INT64) vd4005,
SAFE_CAST(vd4007 AS INT64) vd4007,
SAFE_CAST(vd4008 AS INT64) vd4008,
SAFE_CAST(vd4009 AS INT64) vd4009,
SAFE_CAST(vd4010 AS INT64) vd4010,
SAFE_CAST(vd4011 AS INT64) vd4011,
SAFE_CAST(vd4012 AS INT64) vd4012,
SAFE_CAST(vd4013 AS INT64) vd4013,
SAFE_CAST(vd4014 AS INT64) vd4014,
SAFE_CAST(vd4015 AS INT64) vd4015,
SAFE_CAST(vd4016 AS INT64) vd4016,
SAFE_CAST(vd4017 AS INT64) vd4017,
SAFE_CAST(vd4018 AS INT64) vd4018,
SAFE_CAST(vd4019 AS INT64) vd4019,
SAFE_CAST(vd4020 AS INT64) vd4020,
SAFE_CAST(vd4023 AS INT64) vd4023,
SAFE_CAST(vd4030 AS INT64) vd4030,
SAFE_CAST(vd4031 AS INT64) vd4031,
SAFE_CAST(vd4032 AS INT64) vd4032,
SAFE_CAST(vd4033 AS INT64) vd4033,
SAFE_CAST(vd4034 AS INT64) vd4034,
SAFE_CAST(vd4035 AS INT64) vd4035,
SAFE_CAST(vd4036 AS INT64) vd4036,
SAFE_CAST(vd4037 AS INT64) vd4037,
SAFE_CAST(habitual AS FLOAT64) habitual,
SAFE_CAST(efetivo AS FLOAT64) efetivo
FROM basedosdados-dev.br_ibge_pnadc_staging.microdados AS t