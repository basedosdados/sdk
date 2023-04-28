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

CREATE VIEW basedosdados-dev.world_oecd_pisa.student AS
SELECT 
SAFE_CAST(year AS INT64) year,
SAFE_CAST(country_id_iso_3 AS STRING) country_id_iso_3,
SAFE_CAST(country_id_m49 AS STRING) country_id_m49,
SAFE_CAST(school_id AS STRING) school_id,
SAFE_CAST(student_id AS STRING) student_id,
SAFE_CAST(assessment_type AS STRING) assessment_type,
SAFE_CAST(national_centre_code AS STRING) national_centre_code,
SAFE_CAST(stratum AS STRING) stratum,
SAFE_CAST(subregion AS STRING) subregion,
SAFE_CAST(oecd AS INT64) oecd,
SAFE_CAST(mode_respondent AS STRING) mode_respondent,
SAFE_CAST(language_questionnaire AS STRING) language_questionnaire,
SAFE_CAST(language_assessment AS STRING) language_assessment,
SAFE_CAST(form_id AS STRING) form_id,
SAFE_CAST(international_grade AS STRING) international_grade,
SAFE_CAST(month_birth AS INT64) month_birth,
SAFE_CAST(year_birth AS INT64) year_birth,
SAFE_CAST(gender AS STRING) gender,
SAFE_CAST(effort_put_into_test AS INT64) effort_put_into_test,
SAFE_CAST(effort_would_have_invested AS INT64) effort_would_have_invested,
SAFE_CAST(occupation_mother AS STRING) occupation_mother,
SAFE_CAST(occupation_father AS STRING) occupation_father,
SAFE_CAST(occupation_self AS STRING) occupation_self,
SAFE_CAST(grade_compared AS INT64) grade_compared,
SAFE_CAST(age AS FLOAT64) age,
SAFE_CAST(national_study_programme AS STRING) national_study_programme,
SAFE_CAST(country_birth_self AS STRING) country_birth_self,
SAFE_CAST(country_birth_mother AS STRING) country_birth_mother,
SAFE_CAST(country_birth_father AS STRING) country_birth_father,
SAFE_CAST(language_home AS STRING) language_home,
SAFE_CAST(isced_level AS STRING) isced_level,
SAFE_CAST(isced_designation AS STRING) isced_designation,
SAFE_CAST(isced_orientation AS STRING) isced_orientation,
SAFE_CAST(mother_isced AS STRING) mother_isced,
SAFE_CAST(father_isced AS STRING) father_isced,
SAFE_CAST(highest_parent_isced AS STRING) highest_parent_isced,
SAFE_CAST(index_highest_parent_years_schooling AS FLOAT64) index_highest_parent_years_schooling,
SAFE_CAST(mother_isced_alternate_definition AS STRING) mother_isced_alternate_definition,
SAFE_CAST(father_isced_alternate_definition AS STRING) father_isced_alternate_definition,
SAFE_CAST(highest_parent_isced_alternate_definition AS STRING) highest_parent_isced_alternate_definition,
SAFE_CAST(index_highest_parent_international_years_schooling AS FLOAT64) index_highest_parent_international_years_schooling,
SAFE_CAST(mother_isei AS FLOAT64) mother_isei,
SAFE_CAST(father_isei AS FLOAT64) father_isei,
SAFE_CAST(index_highest_parent_occupation AS FLOAT64) index_highest_parent_occupation,
SAFE_CAST(index_immigration_status AS STRING) index_immigration_status,
SAFE_CAST(duration_early_childhood_education_care AS STRING) duration_early_childhood_education_care,
SAFE_CAST(grade_repetition AS STRING) grade_repetition,
SAFE_CAST(student_occupational_status AS FLOAT64) student_occupational_status,
SAFE_CAST(learning_time_mathematics AS INT64) learning_time_mathematics,
SAFE_CAST(learning_time_language AS INT64) learning_time_language,
SAFE_CAST(learning_time_science AS INT64) learning_time_science,
SAFE_CAST(learning_time_total AS INT64) learning_time_total,
SAFE_CAST(school_change AS STRING) school_change,
SAFE_CAST(number_educational_change AS INT64) number_educational_change,
SAFE_CAST(body_mass_index AS FLOAT64) body_mass_index,
SAFE_CAST(index_economic_social_cultural_status AS FLOAT64) index_economic_social_cultural_status,
SAFE_CAST(meta_cognition_understanding_remembering AS FLOAT64) meta_cognition_understanding_remembering,
SAFE_CAST(meta_cognition_summarising AS FLOAT64) meta_cognition_summarising,
SAFE_CAST(meta_cognition_assess_credibility AS FLOAT64) meta_cognition_assess_credibility,
SAFE_CAST(ict_home AS INT64) ict_home,
SAFE_CAST(ict_school AS INT64) ict_school,
SAFE_CAST(wle_home_possessions AS FLOAT64) wle_home_possessions,
SAFE_CAST(wle_cultural_possessions AS FLOAT64) wle_cultural_possessions,
SAFE_CAST(wle_home_educational_resources AS FLOAT64) wle_home_educational_resources,
SAFE_CAST(wle_family_wealth AS FLOAT64) wle_family_wealth,
SAFE_CAST(wle_ict_resources AS FLOAT64) wle_ict_resources,
SAFE_CAST(wle_teacher_support_language AS FLOAT64) wle_teacher_support_language,
SAFE_CAST(wle_perceived_feedback AS FLOAT64) wle_perceived_feedback,
SAFE_CAST(wle_perceived_parents_emotional_support AS FLOAT64) wle_perceived_parents_emotional_support,
SAFE_CAST(wle_subjective_well_being_sense_belonging AS FLOAT64) wle_subjective_well_being_sense_belonging,
SAFE_CAST(wle_ict_use_outside_school_leisure AS FLOAT64) wle_ict_use_outside_school_leisure,
SAFE_CAST(wle_ict_use_outside_school_work AS FLOAT64) wle_ict_use_outside_school_work,
SAFE_CAST(wle_ict_use_at_school AS FLOAT64) wle_ict_use_at_school,
SAFE_CAST(wle_ict_interest AS FLOAT64) wle_ict_interest,
SAFE_CAST(wle_ict_competence AS FLOAT64) wle_ict_competence,
SAFE_CAST(wle_ict_autonomy AS FLOAT64) wle_ict_autonomy,
SAFE_CAST(wle_ict_social_interaction AS FLOAT64) wle_ict_social_interaction,
SAFE_CAST(wle_parental_support_learning AS FLOAT64) wle_parental_support_learning,
SAFE_CAST(wle_parents_emotional_support AS FLOAT64) wle_parents_emotional_support,
SAFE_CAST(wle_parents_perceived_school_quality AS FLOAT64) wle_parents_perceived_school_quality,
SAFE_CAST(wle_school_policies_parental_involvement AS FLOAT64) wle_school_policies_parental_involvement,
SAFE_CAST(wle_previous_parental_support_learning AS FLOAT64) wle_previous_parental_support_learning,
SAFE_CAST(wle_disciplinay_climate_specific_domain AS FLOAT64) wle_disciplinay_climate_specific_domain,
SAFE_CAST(wle_teacher_directed_instruction AS FLOAT64) wle_teacher_directed_instruction,
SAFE_CAST(wle_teacher_stimulation_reading AS FLOAT64) wle_teacher_stimulation_reading,
SAFE_CAST(wle_adaptation_instruction AS FLOAT64) wle_adaptation_instruction,
SAFE_CAST(wle_perceived_teachers_interest AS FLOAT64) wle_perceived_teachers_interest,
SAFE_CAST(wle_joy_reading AS FLOAT64) wle_joy_reading,
SAFE_CAST(wle_reading_self_concept_competence AS FLOAT64) wle_reading_self_concept_competence,
SAFE_CAST(wle_reading_self_concept_difficulty AS FLOAT64) wle_reading_self_concept_difficulty,
SAFE_CAST(wle_pisa_test_difficulty_perception AS FLOAT64) wle_pisa_test_difficulty_perception,
SAFE_CAST(wle_competitiveness_perception AS FLOAT64) wle_competitiveness_perception,
SAFE_CAST(wle_cooperation_perception AS FLOAT64) wle_cooperation_perception,
SAFE_CAST(wle_attitude_towards_school_learning_activities AS FLOAT64) wle_attitude_towards_school_learning_activities,
SAFE_CAST(wle_competitiveness AS FLOAT64) wle_competitiveness,
SAFE_CAST(wle_work_mastery AS FLOAT64) wle_work_mastery,
SAFE_CAST(wle_general_fear_failure AS FLOAT64) wle_general_fear_failure,
SAFE_CAST(wle_eudaemonia AS FLOAT64) wle_eudaemonia,
SAFE_CAST(wle_subjective_well_being_positive_affect AS FLOAT64) wle_subjective_well_being_positive_affect,
SAFE_CAST(wle_resilence AS FLOAT64) wle_resilence,
SAFE_CAST(wle_mastery_goal_orientation AS FLOAT64) wle_mastery_goal_orientation,
SAFE_CAST(wle_self_efficacy_regarding_global_issues AS FLOAT64) wle_self_efficacy_regarding_global_issues,
SAFE_CAST(wle_students_awareness_global_issues AS FLOAT64) wle_students_awareness_global_issues,
SAFE_CAST(wle_students_attitudes_towards_immigrants AS FLOAT64) wle_students_attitudes_towards_immigrants,
SAFE_CAST(wle_students_interest_learning_other_cultures AS FLOAT64) wle_students_interest_learning_other_cultures,
SAFE_CAST(wle_perspective_taking AS FLOAT64) wle_perspective_taking,
SAFE_CAST(wle_cognitive_flexibility AS FLOAT64) wle_cognitive_flexibility,
SAFE_CAST(wle_respect_people_from_other_cultures AS FLOAT64) wle_respect_people_from_other_cultures,
SAFE_CAST(wle_intercultural_communication_awareness AS FLOAT64) wle_intercultural_communication_awareness,
SAFE_CAST(wle_global_mindedness AS FLOAT64) wle_global_mindedness,
SAFE_CAST(wle_discriminating_school_climate AS FLOAT64) wle_discriminating_school_climate,
SAFE_CAST(wle_students_experience_being_bullied AS FLOAT64) wle_students_experience_being_bullied,
SAFE_CAST(wle_ict_use_during_lessons AS FLOAT64) wle_ict_use_during_lessons,
SAFE_CAST(wle_ict_use_outside_lessons AS FLOAT64) wle_ict_use_outside_lessons,
SAFE_CAST(wle_carrer_information AS FLOAT64) wle_carrer_information,
SAFE_CAST(wle_labour_market_information_by_school AS FLOAT64) wle_labour_market_information_by_school,
SAFE_CAST(wle_labour_market_information_outside_school AS FLOAT64) wle_labour_market_information_outside_school,
SAFE_CAST(wle_financial_matters_confidence AS FLOAT64) wle_financial_matters_confidence,
SAFE_CAST(wle_financial_matters_confidence_digital_devices AS FLOAT64) wle_financial_matters_confidence_digital_devices,
SAFE_CAST(wle_financial_education_school_lessons AS FLOAT64) wle_financial_education_school_lessons,
SAFE_CAST(wle_parents_involvement_financial_literacy AS FLOAT64) wle_parents_involvement_financial_literacy,
SAFE_CAST(wle_parents_reading_enjoyment AS FLOAT64) wle_parents_reading_enjoyment,
SAFE_CAST(wle_parents_attitudes_towards_immigrants AS FLOAT64) wle_parents_attitudes_towards_immigrants,
SAFE_CAST(wle_parents_interest_learking_other_cultures AS FLOAT64) wle_parents_interest_learking_other_cultures,
SAFE_CAST(wle_parents_awareness_global_issues AS FLOAT64) wle_parents_awareness_global_issues,
SAFE_CAST(wle_body_image AS FLOAT64) wle_body_image,
SAFE_CAST(wle_parents_social_connections AS FLOAT64) wle_parents_social_connections,
SAFE_CAST(final_student_weight AS FLOAT64) final_student_weight,
SAFE_CAST(student_weight_1 AS FLOAT64) student_weight_1,
SAFE_CAST(student_weight_2 AS FLOAT64) student_weight_2,
SAFE_CAST(student_weight_3 AS FLOAT64) student_weight_3,
SAFE_CAST(student_weight_4 AS FLOAT64) student_weight_4,
SAFE_CAST(student_weight_5 AS FLOAT64) student_weight_5,
SAFE_CAST(student_weight_6 AS FLOAT64) student_weight_6,
SAFE_CAST(student_weight_7 AS FLOAT64) student_weight_7,
SAFE_CAST(student_weight_8 AS FLOAT64) student_weight_8,
SAFE_CAST(student_weight_9 AS FLOAT64) student_weight_9,
SAFE_CAST(student_weight_10 AS FLOAT64) student_weight_10,
SAFE_CAST(student_weight_11 AS FLOAT64) student_weight_11,
SAFE_CAST(student_weight_12 AS FLOAT64) student_weight_12,
SAFE_CAST(student_weight_13 AS FLOAT64) student_weight_13,
SAFE_CAST(student_weight_14 AS FLOAT64) student_weight_14,
SAFE_CAST(student_weight_15 AS FLOAT64) student_weight_15,
SAFE_CAST(student_weight_16 AS FLOAT64) student_weight_16,
SAFE_CAST(student_weight_17 AS FLOAT64) student_weight_17,
SAFE_CAST(student_weight_18 AS FLOAT64) student_weight_18,
SAFE_CAST(student_weight_19 AS FLOAT64) student_weight_19,
SAFE_CAST(student_weight_20 AS FLOAT64) student_weight_20,
SAFE_CAST(student_weight_21 AS FLOAT64) student_weight_21,
SAFE_CAST(student_weight_22 AS FLOAT64) student_weight_22,
SAFE_CAST(student_weight_23 AS FLOAT64) student_weight_23,
SAFE_CAST(student_weight_24 AS FLOAT64) student_weight_24,
SAFE_CAST(student_weight_25 AS FLOAT64) student_weight_25,
SAFE_CAST(student_weight_26 AS FLOAT64) student_weight_26,
SAFE_CAST(student_weight_27 AS FLOAT64) student_weight_27,
SAFE_CAST(student_weight_28 AS FLOAT64) student_weight_28,
SAFE_CAST(student_weight_29 AS FLOAT64) student_weight_29,
SAFE_CAST(student_weight_30 AS FLOAT64) student_weight_30,
SAFE_CAST(student_weight_31 AS FLOAT64) student_weight_31,
SAFE_CAST(student_weight_32 AS FLOAT64) student_weight_32,
SAFE_CAST(student_weight_33 AS FLOAT64) student_weight_33,
SAFE_CAST(student_weight_34 AS FLOAT64) student_weight_34,
SAFE_CAST(student_weight_35 AS FLOAT64) student_weight_35,
SAFE_CAST(student_weight_36 AS FLOAT64) student_weight_36,
SAFE_CAST(student_weight_37 AS FLOAT64) student_weight_37,
SAFE_CAST(student_weight_38 AS FLOAT64) student_weight_38,
SAFE_CAST(student_weight_39 AS FLOAT64) student_weight_39,
SAFE_CAST(student_weight_40 AS FLOAT64) student_weight_40,
SAFE_CAST(student_weight_41 AS FLOAT64) student_weight_41,
SAFE_CAST(student_weight_42 AS FLOAT64) student_weight_42,
SAFE_CAST(student_weight_43 AS FLOAT64) student_weight_43,
SAFE_CAST(student_weight_44 AS FLOAT64) student_weight_44,
SAFE_CAST(student_weight_45 AS FLOAT64) student_weight_45,
SAFE_CAST(student_weight_46 AS FLOAT64) student_weight_46,
SAFE_CAST(student_weight_47 AS FLOAT64) student_weight_47,
SAFE_CAST(student_weight_48 AS FLOAT64) student_weight_48,
SAFE_CAST(student_weight_49 AS FLOAT64) student_weight_49,
SAFE_CAST(student_weight_50 AS FLOAT64) student_weight_50,
SAFE_CAST(student_weight_51 AS FLOAT64) student_weight_51,
SAFE_CAST(student_weight_52 AS FLOAT64) student_weight_52,
SAFE_CAST(student_weight_53 AS FLOAT64) student_weight_53,
SAFE_CAST(student_weight_54 AS FLOAT64) student_weight_54,
SAFE_CAST(student_weight_55 AS FLOAT64) student_weight_55,
SAFE_CAST(student_weight_56 AS FLOAT64) student_weight_56,
SAFE_CAST(student_weight_57 AS FLOAT64) student_weight_57,
SAFE_CAST(student_weight_58 AS FLOAT64) student_weight_58,
SAFE_CAST(student_weight_59 AS FLOAT64) student_weight_59,
SAFE_CAST(student_weight_60 AS FLOAT64) student_weight_60,
SAFE_CAST(student_weight_61 AS FLOAT64) student_weight_61,
SAFE_CAST(student_weight_62 AS FLOAT64) student_weight_62,
SAFE_CAST(student_weight_63 AS FLOAT64) student_weight_63,
SAFE_CAST(student_weight_64 AS FLOAT64) student_weight_64,
SAFE_CAST(student_weight_65 AS FLOAT64) student_weight_65,
SAFE_CAST(student_weight_66 AS FLOAT64) student_weight_66,
SAFE_CAST(student_weight_67 AS FLOAT64) student_weight_67,
SAFE_CAST(student_weight_68 AS FLOAT64) student_weight_68,
SAFE_CAST(student_weight_69 AS FLOAT64) student_weight_69,
SAFE_CAST(student_weight_70 AS FLOAT64) student_weight_70,
SAFE_CAST(student_weight_71 AS FLOAT64) student_weight_71,
SAFE_CAST(student_weight_72 AS FLOAT64) student_weight_72,
SAFE_CAST(student_weight_73 AS FLOAT64) student_weight_73,
SAFE_CAST(student_weight_74 AS FLOAT64) student_weight_74,
SAFE_CAST(student_weight_75 AS FLOAT64) student_weight_75,
SAFE_CAST(student_weight_76 AS FLOAT64) student_weight_76,
SAFE_CAST(student_weight_77 AS FLOAT64) student_weight_77,
SAFE_CAST(student_weight_78 AS FLOAT64) student_weight_78,
SAFE_CAST(student_weight_79 AS FLOAT64) student_weight_79,
SAFE_CAST(student_weight_80 AS FLOAT64) student_weight_80,
SAFE_CAST(randomly_assigned_unit_number AS INT64) randomly_assigned_unit_number,
SAFE_CAST(randomized_final_variance_stratum AS INT64) randomized_final_variance_stratum,
SAFE_CAST(plausible_value_1_mathematics AS FLOAT64) plausible_value_1_mathematics,
SAFE_CAST(plausible_value_2_mathematics AS FLOAT64) plausible_value_2_mathematics,
SAFE_CAST(plausible_value_3_mathematics AS FLOAT64) plausible_value_3_mathematics,
SAFE_CAST(plausible_value_4_mathematics AS FLOAT64) plausible_value_4_mathematics,
SAFE_CAST(plausible_value_5_mathematics AS FLOAT64) plausible_value_5_mathematics,
SAFE_CAST(plausible_value_6_mathematics AS FLOAT64) plausible_value_6_mathematics,
SAFE_CAST(plausible_value_7_mathematics AS FLOAT64) plausible_value_7_mathematics,
SAFE_CAST(plausible_value_8_mathematics AS FLOAT64) plausible_value_8_mathematics,
SAFE_CAST(plausible_value_9_mathematics AS FLOAT64) plausible_value_9_mathematics,
SAFE_CAST(plausible_value_10_mathematics AS FLOAT64) plausible_value_10_mathematics,
SAFE_CAST(plausible_value_1_reading AS FLOAT64) plausible_value_1_reading,
SAFE_CAST(plausible_value_2_reading AS FLOAT64) plausible_value_2_reading,
SAFE_CAST(plausible_value_3_reading AS FLOAT64) plausible_value_3_reading,
SAFE_CAST(plausible_value_4_reading AS FLOAT64) plausible_value_4_reading,
SAFE_CAST(plausible_value_5_reading AS FLOAT64) plausible_value_5_reading,
SAFE_CAST(plausible_value_6_reading AS FLOAT64) plausible_value_6_reading,
SAFE_CAST(plausible_value_7_reading AS FLOAT64) plausible_value_7_reading,
SAFE_CAST(plausible_value_8_reading AS FLOAT64) plausible_value_8_reading,
SAFE_CAST(plausible_value_9_reading AS FLOAT64) plausible_value_9_reading,
SAFE_CAST(plausible_value_10_reading AS FLOAT64) plausible_value_10_reading,
SAFE_CAST(plausible_value_1_science AS FLOAT64) plausible_value_1_science,
SAFE_CAST(plausible_value_2_science AS FLOAT64) plausible_value_2_science,
SAFE_CAST(plausible_value_3_science AS FLOAT64) plausible_value_3_science,
SAFE_CAST(plausible_value_4_science AS FLOAT64) plausible_value_4_science,
SAFE_CAST(plausible_value_5_science AS FLOAT64) plausible_value_5_science,
SAFE_CAST(plausible_value_6_science AS FLOAT64) plausible_value_6_science,
SAFE_CAST(plausible_value_7_science AS FLOAT64) plausible_value_7_science,
SAFE_CAST(plausible_value_8_science AS FLOAT64) plausible_value_8_science,
SAFE_CAST(plausible_value_9_science AS FLOAT64) plausible_value_9_science,
SAFE_CAST(plausible_value_10_science AS FLOAT64) plausible_value_10_science,
SAFE_CAST(senate_weight AS FLOAT64) senate_weight,
SAFE_CAST(date_database_creation AS DATE) date_database_creation
FROM basedosdados-dev.world_oecd_pisa_staging.student AS t