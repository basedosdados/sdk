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

CREATE VIEW basedosdados-dev.br_inep_indicadores_educacionais.regiao AS
SELECT 
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(regiao AS STRING) regiao,
SAFE_CAST(localizacao AS STRING) localizacao,
SAFE_CAST(rede AS STRING) rede,
SAFE_CAST(atu_ei AS FLOAT64) atu_ei,
SAFE_CAST(atu_ei_creche AS FLOAT64) atu_ei_creche,
SAFE_CAST(atu_ei_pre_escola AS FLOAT64) atu_ei_pre_escola,
SAFE_CAST(atu_ef AS FLOAT64) atu_ef,
SAFE_CAST(atu_ef_anos_iniciais AS FLOAT64) atu_ef_anos_iniciais,
SAFE_CAST(atu_ef_anos_finais AS FLOAT64) atu_ef_anos_finais,
SAFE_CAST(atu_ef_1_ano AS FLOAT64) atu_ef_1_ano,
SAFE_CAST(atu_ef_2_ano AS FLOAT64) atu_ef_2_ano,
SAFE_CAST(atu_ef_3_ano AS FLOAT64) atu_ef_3_ano,
SAFE_CAST(atu_ef_4_ano AS FLOAT64) atu_ef_4_ano,
SAFE_CAST(atu_ef_5_ano AS FLOAT64) atu_ef_5_ano,
SAFE_CAST(atu_ef_6_ano AS FLOAT64) atu_ef_6_ano,
SAFE_CAST(atu_ef_7_ano AS FLOAT64) atu_ef_7_ano,
SAFE_CAST(atu_ef_8_ano AS FLOAT64) atu_ef_8_ano,
SAFE_CAST(atu_ef_9_ano AS FLOAT64) atu_ef_9_ano,
SAFE_CAST(atu_ef_turmas_unif_multi_fluxo AS FLOAT64) atu_ef_turmas_unif_multi_fluxo,
SAFE_CAST(atu_em AS FLOAT64) atu_em,
SAFE_CAST(atu_em_1_ano AS FLOAT64) atu_em_1_ano,
SAFE_CAST(atu_em_2_ano AS FLOAT64) atu_em_2_ano,
SAFE_CAST(atu_em_3_ano AS FLOAT64) atu_em_3_ano,
SAFE_CAST(atu_em_4_ano AS FLOAT64) atu_em_4_ano,
SAFE_CAST(atu_em_nao_seriado AS FLOAT64) atu_em_nao_seriado,
SAFE_CAST(had_ei AS STRING) had_ei,
SAFE_CAST(had_ei_creche AS FLOAT64) had_ei_creche,
SAFE_CAST(had_ei_pre_escola AS FLOAT64) had_ei_pre_escola,
SAFE_CAST(had_ef AS FLOAT64) had_ef,
SAFE_CAST(had_ef_anos_iniciais AS FLOAT64) had_ef_anos_iniciais,
SAFE_CAST(had_ef_anos_finais AS FLOAT64) had_ef_anos_finais,
SAFE_CAST(had_ef_1_ano AS FLOAT64) had_ef_1_ano,
SAFE_CAST(had_ef_2_ano AS FLOAT64) had_ef_2_ano,
SAFE_CAST(had_ef_3_ano AS FLOAT64) had_ef_3_ano,
SAFE_CAST(had_ef_4_ano AS FLOAT64) had_ef_4_ano,
SAFE_CAST(had_ef_5_ano AS FLOAT64) had_ef_5_ano,
SAFE_CAST(had_ef_6_ano AS FLOAT64) had_ef_6_ano,
SAFE_CAST(had_ef_7_ano AS FLOAT64) had_ef_7_ano,
SAFE_CAST(had_ef_8_ano AS FLOAT64) had_ef_8_ano,
SAFE_CAST(had_ef_9_ano AS FLOAT64) had_ef_9_ano,
SAFE_CAST(had_em AS FLOAT64) had_em,
SAFE_CAST(had_em_1_ano AS FLOAT64) had_em_1_ano,
SAFE_CAST(had_em_2_ano AS FLOAT64) had_em_2_ano,
SAFE_CAST(had_em_3_ano AS FLOAT64) had_em_3_ano,
SAFE_CAST(had_em_4_ano AS FLOAT64) had_em_4_ano,
SAFE_CAST(had_em_nao_seriado AS FLOAT64) had_em_nao_seriado,
SAFE_CAST(tdi_ef AS FLOAT64) tdi_ef,
SAFE_CAST(tdi_ef_anos_iniciais AS FLOAT64) tdi_ef_anos_iniciais,
SAFE_CAST(tdi_ef_anos_finais AS FLOAT64) tdi_ef_anos_finais,
SAFE_CAST(tdi_ef_1_ano AS FLOAT64) tdi_ef_1_ano,
SAFE_CAST(tdi_ef_2_ano AS FLOAT64) tdi_ef_2_ano,
SAFE_CAST(tdi_ef_3_ano AS FLOAT64) tdi_ef_3_ano,
SAFE_CAST(tdi_ef_4_ano AS FLOAT64) tdi_ef_4_ano,
SAFE_CAST(tdi_ef_5_ano AS FLOAT64) tdi_ef_5_ano,
SAFE_CAST(tdi_ef_6_ano AS FLOAT64) tdi_ef_6_ano,
SAFE_CAST(tdi_ef_7_ano AS FLOAT64) tdi_ef_7_ano,
SAFE_CAST(tdi_ef_8_ano AS FLOAT64) tdi_ef_8_ano,
SAFE_CAST(tdi_ef_9_ano AS FLOAT64) tdi_ef_9_ano,
SAFE_CAST(tdi_em AS FLOAT64) tdi_em,
SAFE_CAST(tdi_em_1_ano AS FLOAT64) tdi_em_1_ano,
SAFE_CAST(tdi_em_2_ano AS FLOAT64) tdi_em_2_ano,
SAFE_CAST(tdi_em_3_ano AS FLOAT64) tdi_em_3_ano,
SAFE_CAST(tdi_em_4_ano AS FLOAT64) tdi_em_4_ano,
SAFE_CAST(taxa_aprovacao_ef AS FLOAT64) taxa_aprovacao_ef,
SAFE_CAST(taxa_aprovacao_ef_anos_iniciais AS FLOAT64) taxa_aprovacao_ef_anos_iniciais,
SAFE_CAST(taxa_aprovacao_ef_anos_finais AS FLOAT64) taxa_aprovacao_ef_anos_finais,
SAFE_CAST(taxa_aprovacao_ef_1_ano AS FLOAT64) taxa_aprovacao_ef_1_ano,
SAFE_CAST(taxa_aprovacao_ef_2_ano AS FLOAT64) taxa_aprovacao_ef_2_ano,
SAFE_CAST(taxa_aprovacao_ef_3_ano AS FLOAT64) taxa_aprovacao_ef_3_ano,
SAFE_CAST(taxa_aprovacao_ef_4_ano AS FLOAT64) taxa_aprovacao_ef_4_ano,
SAFE_CAST(taxa_aprovacao_ef_5_ano AS FLOAT64) taxa_aprovacao_ef_5_ano,
SAFE_CAST(taxa_aprovacao_ef_6_ano AS FLOAT64) taxa_aprovacao_ef_6_ano,
SAFE_CAST(taxa_aprovacao_ef_7_ano AS FLOAT64) taxa_aprovacao_ef_7_ano,
SAFE_CAST(taxa_aprovacao_ef_8_ano AS FLOAT64) taxa_aprovacao_ef_8_ano,
SAFE_CAST(taxa_aprovacao_ef_9_ano AS FLOAT64) taxa_aprovacao_ef_9_ano,
SAFE_CAST(taxa_aprovacao_em AS FLOAT64) taxa_aprovacao_em,
SAFE_CAST(taxa_aprovacao_em_1_ano AS FLOAT64) taxa_aprovacao_em_1_ano,
SAFE_CAST(taxa_aprovacao_em_2_ano AS FLOAT64) taxa_aprovacao_em_2_ano,
SAFE_CAST(taxa_aprovacao_em_3_ano AS FLOAT64) taxa_aprovacao_em_3_ano,
SAFE_CAST(taxa_aprovacao_em_4_ano AS FLOAT64) taxa_aprovacao_em_4_ano,
SAFE_CAST(taxa_aprovacao_em_nao_seriado AS FLOAT64) taxa_aprovacao_em_nao_seriado,
SAFE_CAST(taxa_reprovacao_ef AS FLOAT64) taxa_reprovacao_ef,
SAFE_CAST(taxa_reprovacao_ef_anos_iniciais AS FLOAT64) taxa_reprovacao_ef_anos_iniciais,
SAFE_CAST(taxa_reprovacao_ef_anos_finais AS FLOAT64) taxa_reprovacao_ef_anos_finais,
SAFE_CAST(taxa_reprovacao_ef_1_ano AS FLOAT64) taxa_reprovacao_ef_1_ano,
SAFE_CAST(taxa_reprovacao_ef_2_ano AS FLOAT64) taxa_reprovacao_ef_2_ano,
SAFE_CAST(taxa_reprovacao_ef_3_ano AS FLOAT64) taxa_reprovacao_ef_3_ano,
SAFE_CAST(taxa_reprovacao_ef_4_ano AS FLOAT64) taxa_reprovacao_ef_4_ano,
SAFE_CAST(taxa_reprovacao_ef_5_ano AS FLOAT64) taxa_reprovacao_ef_5_ano,
SAFE_CAST(taxa_reprovacao_ef_6_ano AS FLOAT64) taxa_reprovacao_ef_6_ano,
SAFE_CAST(taxa_reprovacao_ef_7_ano AS FLOAT64) taxa_reprovacao_ef_7_ano,
SAFE_CAST(taxa_reprovacao_ef_8_ano AS FLOAT64) taxa_reprovacao_ef_8_ano,
SAFE_CAST(taxa_reprovacao_ef_9_ano AS FLOAT64) taxa_reprovacao_ef_9_ano,
SAFE_CAST(taxa_reprovacao_em AS FLOAT64) taxa_reprovacao_em,
SAFE_CAST(taxa_reprovacao_em_1_ano AS FLOAT64) taxa_reprovacao_em_1_ano,
SAFE_CAST(taxa_reprovacao_em_2_ano AS FLOAT64) taxa_reprovacao_em_2_ano,
SAFE_CAST(taxa_reprovacao_em_3_ano AS FLOAT64) taxa_reprovacao_em_3_ano,
SAFE_CAST(taxa_reprovacao_em_4_ano AS FLOAT64) taxa_reprovacao_em_4_ano,
SAFE_CAST(taxa_reprovacao_em_nao_seriado AS FLOAT64) taxa_reprovacao_em_nao_seriado,
SAFE_CAST(taxa_abandono_ef AS FLOAT64) taxa_abandono_ef,
SAFE_CAST(taxa_abandono_ef_anos_iniciais AS FLOAT64) taxa_abandono_ef_anos_iniciais,
SAFE_CAST(taxa_abandono_ef_anos_finais AS FLOAT64) taxa_abandono_ef_anos_finais,
SAFE_CAST(taxa_abandono_ef_1_ano AS FLOAT64) taxa_abandono_ef_1_ano,
SAFE_CAST(taxa_abandono_ef_2_ano AS FLOAT64) taxa_abandono_ef_2_ano,
SAFE_CAST(taxa_abandono_ef_3_ano AS FLOAT64) taxa_abandono_ef_3_ano,
SAFE_CAST(taxa_abandono_ef_4_ano AS FLOAT64) taxa_abandono_ef_4_ano,
SAFE_CAST(taxa_abandono_ef_5_ano AS FLOAT64) taxa_abandono_ef_5_ano,
SAFE_CAST(taxa_abandono_ef_6_ano AS FLOAT64) taxa_abandono_ef_6_ano,
SAFE_CAST(taxa_abandono_ef_7_ano AS FLOAT64) taxa_abandono_ef_7_ano,
SAFE_CAST(taxa_abandono_ef_8_ano AS FLOAT64) taxa_abandono_ef_8_ano,
SAFE_CAST(taxa_abandono_ef_9_ano AS FLOAT64) taxa_abandono_ef_9_ano,
SAFE_CAST(taxa_abandono_em AS FLOAT64) taxa_abandono_em,
SAFE_CAST(taxa_abandono_em_1_ano AS FLOAT64) taxa_abandono_em_1_ano,
SAFE_CAST(taxa_abandono_em_2_ano AS FLOAT64) taxa_abandono_em_2_ano,
SAFE_CAST(taxa_abandono_em_3_ano AS FLOAT64) taxa_abandono_em_3_ano,
SAFE_CAST(taxa_abandono_em_4_ano AS FLOAT64) taxa_abandono_em_4_ano,
SAFE_CAST(taxa_abandono_em_nao_seriado AS FLOAT64) taxa_abandono_em_nao_seriado,
SAFE_CAST(tnr_ef AS FLOAT64) tnr_ef,
SAFE_CAST(tnr_ef_anos_iniciais AS FLOAT64) tnr_ef_anos_iniciais,
SAFE_CAST(tnr_ef_anos_finais AS FLOAT64) tnr_ef_anos_finais,
SAFE_CAST(tnr_ef_1_ano AS FLOAT64) tnr_ef_1_ano,
SAFE_CAST(tnr_ef_2_ano AS FLOAT64) tnr_ef_2_ano,
SAFE_CAST(tnr_ef_3_ano AS FLOAT64) tnr_ef_3_ano,
SAFE_CAST(tnr_ef_4_ano AS FLOAT64) tnr_ef_4_ano,
SAFE_CAST(tnr_ef_5_ano AS FLOAT64) tnr_ef_5_ano,
SAFE_CAST(tnr_ef_6_ano AS FLOAT64) tnr_ef_6_ano,
SAFE_CAST(tnr_ef_7_ano AS FLOAT64) tnr_ef_7_ano,
SAFE_CAST(tnr_ef_8_ano AS FLOAT64) tnr_ef_8_ano,
SAFE_CAST(tnr_ef_9_ano AS FLOAT64) tnr_ef_9_ano,
SAFE_CAST(tnr_em AS FLOAT64) tnr_em,
SAFE_CAST(tnr_em_1_ano AS FLOAT64) tnr_em_1_ano,
SAFE_CAST(tnr_em_2_ano AS FLOAT64) tnr_em_2_ano,
SAFE_CAST(tnr_em_3_ano AS FLOAT64) tnr_em_3_ano,
SAFE_CAST(tnr_em_4_ano AS FLOAT64) tnr_em_4_ano,
SAFE_CAST(tnr_em_nao_seriado AS FLOAT64) tnr_em_nao_seriado,
SAFE_CAST(dsu_ei AS FLOAT64) dsu_ei,
SAFE_CAST(dsu_ei_creche AS FLOAT64) dsu_ei_creche,
SAFE_CAST(dsu_ei_pre_escola AS FLOAT64) dsu_ei_pre_escola,
SAFE_CAST(dsu_ef AS FLOAT64) dsu_ef,
SAFE_CAST(dsu_ef_anos_iniciais AS FLOAT64) dsu_ef_anos_iniciais,
SAFE_CAST(dsu_ef_anos_finais AS FLOAT64) dsu_ef_anos_finais,
SAFE_CAST(dsu_em AS FLOAT64) dsu_em,
SAFE_CAST(dsu_ep AS FLOAT64) dsu_ep,
SAFE_CAST(dsu_eja AS FLOAT64) dsu_eja,
SAFE_CAST(dsu_ee AS FLOAT64) dsu_ee,
SAFE_CAST(afd_ei_grupo_1 AS FLOAT64) afd_ei_grupo_1,
SAFE_CAST(afd_ei_grupo_2 AS FLOAT64) afd_ei_grupo_2,
SAFE_CAST(afd_ei_grupo_3 AS FLOAT64) afd_ei_grupo_3,
SAFE_CAST(afd_ei_grupo_4 AS FLOAT64) afd_ei_grupo_4,
SAFE_CAST(afd_ei_grupo_5 AS FLOAT64) afd_ei_grupo_5,
SAFE_CAST(afd_ef_grupo_1 AS FLOAT64) afd_ef_grupo_1,
SAFE_CAST(afd_ef_grupo_2 AS FLOAT64) afd_ef_grupo_2,
SAFE_CAST(afd_ef_grupo_3 AS FLOAT64) afd_ef_grupo_3,
SAFE_CAST(afd_ef_grupo_4 AS FLOAT64) afd_ef_grupo_4,
SAFE_CAST(afd_ef_grupo_5 AS FLOAT64) afd_ef_grupo_5,
SAFE_CAST(afd_ef_anos_iniciais_grupo_1 AS FLOAT64) afd_ef_anos_iniciais_grupo_1,
SAFE_CAST(afd_ef_anos_iniciais_grupo_2 AS FLOAT64) afd_ef_anos_iniciais_grupo_2,
SAFE_CAST(afd_ef_anos_iniciais_grupo_3 AS FLOAT64) afd_ef_anos_iniciais_grupo_3,
SAFE_CAST(afd_ef_anos_iniciais_grupo_4 AS FLOAT64) afd_ef_anos_iniciais_grupo_4,
SAFE_CAST(afd_ef_anos_iniciais_grupo_5 AS FLOAT64) afd_ef_anos_iniciais_grupo_5,
SAFE_CAST(afd_ef_anos_finais_grupo_1 AS FLOAT64) afd_ef_anos_finais_grupo_1,
SAFE_CAST(afd_ef_anos_finais_grupo_2 AS FLOAT64) afd_ef_anos_finais_grupo_2,
SAFE_CAST(afd_ef_anos_finais_grupo_3 AS FLOAT64) afd_ef_anos_finais_grupo_3,
SAFE_CAST(afd_ef_anos_finais_grupo_4 AS FLOAT64) afd_ef_anos_finais_grupo_4,
SAFE_CAST(afd_ef_anos_finais_grupo_5 AS FLOAT64) afd_ef_anos_finais_grupo_5,
SAFE_CAST(afd_em_grupo_1 AS FLOAT64) afd_em_grupo_1,
SAFE_CAST(afd_em_grupo_2 AS FLOAT64) afd_em_grupo_2,
SAFE_CAST(afd_em_grupo_3 AS FLOAT64) afd_em_grupo_3,
SAFE_CAST(afd_em_grupo_4 AS FLOAT64) afd_em_grupo_4,
SAFE_CAST(afd_em_grupo_5 AS FLOAT64) afd_em_grupo_5,
SAFE_CAST(afd_eja_fundamental_grupo_1 AS FLOAT64) afd_eja_fundamental_grupo_1,
SAFE_CAST(afd_eja_fundamental_grupo_2 AS FLOAT64) afd_eja_fundamental_grupo_2,
SAFE_CAST(afd_eja_fundamental_grupo_3 AS FLOAT64) afd_eja_fundamental_grupo_3,
SAFE_CAST(afd_eja_fundamental_grupo_4 AS FLOAT64) afd_eja_fundamental_grupo_4,
SAFE_CAST(afd_eja_fundamental_grupo_5 AS FLOAT64) afd_eja_fundamental_grupo_5,
SAFE_CAST(afd_eja_medio_grupo_1 AS FLOAT64) afd_eja_medio_grupo_1,
SAFE_CAST(afd_eja_medio_grupo_2 AS FLOAT64) afd_eja_medio_grupo_2,
SAFE_CAST(afd_eja_medio_grupo_3 AS FLOAT64) afd_eja_medio_grupo_3,
SAFE_CAST(afd_eja_medio_grupo_4 AS FLOAT64) afd_eja_medio_grupo_4,
SAFE_CAST(afd_eja_medio_grupo_5 AS FLOAT64) afd_eja_medio_grupo_5,
SAFE_CAST(ird_baixa_regularidade AS FLOAT64) ird_baixa_regularidade,
SAFE_CAST(ird_media_baixa AS FLOAT64) ird_media_baixa,
SAFE_CAST(ird_media_alta AS FLOAT64) ird_media_alta,
SAFE_CAST(ird_alta AS FLOAT64) ird_alta,
SAFE_CAST(ied_ef_nivel_1 AS FLOAT64) ied_ef_nivel_1,
SAFE_CAST(ied_ef_nivel_2 AS FLOAT64) ied_ef_nivel_2,
SAFE_CAST(ied_ef_nivel_3 AS FLOAT64) ied_ef_nivel_3,
SAFE_CAST(ied_ef_nivel_4 AS FLOAT64) ied_ef_nivel_4,
SAFE_CAST(ied_ef_nivel_5 AS FLOAT64) ied_ef_nivel_5,
SAFE_CAST(ied_ef_nivel_6 AS FLOAT64) ied_ef_nivel_6,
SAFE_CAST(ied_ef_anos_iniciais_nivel_1 AS FLOAT64) ied_ef_anos_iniciais_nivel_1,
SAFE_CAST(ied_ef_anos_iniciais_nivel_2 AS FLOAT64) ied_ef_anos_iniciais_nivel_2,
SAFE_CAST(ied_ef_anos_iniciais_nivel_3 AS FLOAT64) ied_ef_anos_iniciais_nivel_3,
SAFE_CAST(ied_ef_anos_iniciais_nivel_4 AS FLOAT64) ied_ef_anos_iniciais_nivel_4,
SAFE_CAST(ied_ef_anos_iniciais_nivel_5 AS FLOAT64) ied_ef_anos_iniciais_nivel_5,
SAFE_CAST(ied_ef_anos_iniciais_nivel_6 AS FLOAT64) ied_ef_anos_iniciais_nivel_6,
SAFE_CAST(ied_ef_anos_finais_nivel_1 AS FLOAT64) ied_ef_anos_finais_nivel_1,
SAFE_CAST(ied_ef_anos_finais_nivel_2 AS FLOAT64) ied_ef_anos_finais_nivel_2,
SAFE_CAST(ied_ef_anos_finais_nivel_3 AS FLOAT64) ied_ef_anos_finais_nivel_3,
SAFE_CAST(ied_ef_anos_finais_nivel_4 AS FLOAT64) ied_ef_anos_finais_nivel_4,
SAFE_CAST(ied_ef_anos_finais_nivel_5 AS FLOAT64) ied_ef_anos_finais_nivel_5,
SAFE_CAST(ied_ef_anos_finais_nivel_6 AS FLOAT64) ied_ef_anos_finais_nivel_6,
SAFE_CAST(ied_em_nivel_1 AS FLOAT64) ied_em_nivel_1,
SAFE_CAST(ied_em_nivel_2 AS FLOAT64) ied_em_nivel_2,
SAFE_CAST(ied_em_nivel_3 AS FLOAT64) ied_em_nivel_3,
SAFE_CAST(ied_em_nivel_4 AS FLOAT64) ied_em_nivel_4,
SAFE_CAST(ied_em_nivel_5 AS FLOAT64) ied_em_nivel_5,
SAFE_CAST(ied_em_nivel_6 AS FLOAT64) ied_em_nivel_6,
SAFE_CAST(icg_nivel_1 AS FLOAT64) icg_nivel_1,
SAFE_CAST(icg_nivel_2 AS FLOAT64) icg_nivel_2,
SAFE_CAST(icg_nivel_3 AS FLOAT64) icg_nivel_3,
SAFE_CAST(icg_nivel_4 AS FLOAT64) icg_nivel_4,
SAFE_CAST(icg_nivel_5 AS FLOAT64) icg_nivel_5,
SAFE_CAST(icg_nivel_6 AS FLOAT64) icg_nivel_6
FROM basedosdados-dev.br_inep_indicadores_educacionais_staging.regiao AS t