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

CREATE VIEW basedosdados-dev.world_oecd_public_finance.country_data AS
SELECT 
SAFE_CAST(year AS INT64) year,
SAFE_CAST(country AS STRING) country,
SAFE_CAST(revenue_personal_income_tax AS FLOAT64) revenue_personal_income_tax,
SAFE_CAST(revenue_social_security_contribution AS FLOAT64) revenue_social_security_contribution,
SAFE_CAST(revenue_corporate_tax AS FLOAT64) revenue_corporate_tax,
SAFE_CAST(revenue_environmental_tax AS FLOAT64) revenue_environmental_tax,
SAFE_CAST(revenue_other_consumption_tax AS FLOAT64) revenue_other_consumption_tax,
SAFE_CAST(revenue_immovable_property_tax AS FLOAT64) revenue_immovable_property_tax,
SAFE_CAST(revenue_other_property_tax AS FLOAT64) revenue_other_property_tax,
SAFE_CAST(revenue_sales_goods_services_tax AS FLOAT64) revenue_sales_goods_services_tax,
SAFE_CAST(revenue_other_non_property_tax AS FLOAT64) revenue_other_non_property_tax,
SAFE_CAST(revenue_property_income AS FLOAT64) revenue_property_income,
SAFE_CAST(revenue_property_income_except_interest AS FLOAT64) revenue_property_income_except_interest,
SAFE_CAST(expenditure_education AS FLOAT64) expenditure_education,
SAFE_CAST(expenditure_health AS FLOAT64) expenditure_health,
SAFE_CAST(expenditure_wage_intermediate_consumption AS FLOAT64) expenditure_wage_intermediate_consumption,
SAFE_CAST(expenditure_pension AS FLOAT64) expenditure_pension,
SAFE_CAST(expenditure_sickness_disability AS FLOAT64) expenditure_sickness_disability,
SAFE_CAST(expenditure_unemployment_benefit AS FLOAT64) expenditure_unemployment_benefit,
SAFE_CAST(expenditure_family_children AS FLOAT64) expenditure_family_children,
SAFE_CAST(expenditure_subsidies AS FLOAT64) expenditure_subsidies,
SAFE_CAST(expenditure_public_investment AS FLOAT64) expenditure_public_investment,
SAFE_CAST(expenditure_other_primary_expenditure AS FLOAT64) expenditure_other_primary_expenditure,
SAFE_CAST(expenditure_property_income AS FLOAT64) expenditure_property_income,
SAFE_CAST(expenditure_property_income_excep_interest AS FLOAT64) expenditure_property_income_excep_interest,
SAFE_CAST(revenue_personal_income_tax_adjusted AS FLOAT64) revenue_personal_income_tax_adjusted,
SAFE_CAST(revenue_social_security_contribution_adjusted AS FLOAT64) revenue_social_security_contribution_adjusted,
SAFE_CAST(revenue_corporate_tax_adjusted AS FLOAT64) revenue_corporate_tax_adjusted,
SAFE_CAST(revenue_environmental_tax_adjusted AS FLOAT64) revenue_environmental_tax_adjusted,
SAFE_CAST(revenue_other_consumption_tax_adjusted AS FLOAT64) revenue_other_consumption_tax_adjusted,
SAFE_CAST(revenue_immovable_property_tax_adjusted AS FLOAT64) revenue_immovable_property_tax_adjusted,
SAFE_CAST(revenue_other_property_tax_adjusted AS FLOAT64) revenue_other_property_tax_adjusted,
SAFE_CAST(revenue_sales_goods_services_tax_adjusted AS FLOAT64) revenue_sales_goods_services_tax_adjusted,
SAFE_CAST(revenue_other_non_property_tax_adjusted AS FLOAT64) revenue_other_non_property_tax_adjusted,
SAFE_CAST(revenue_property_income_adjusted AS FLOAT64) revenue_property_income_adjusted,
SAFE_CAST(revenue_property_income_except_interest_adjusted AS FLOAT64) revenue_property_income_except_interest_adjusted,
SAFE_CAST(expenditure_education_adjusted AS FLOAT64) expenditure_education_adjusted,
SAFE_CAST(expenditure_health_adjusted AS FLOAT64) expenditure_health_adjusted,
SAFE_CAST(expenditure_wage_intermediate_consumption_adjusted AS FLOAT64) expenditure_wage_intermediate_consumption_adjusted,
SAFE_CAST(expenditure_pension_adjusted AS FLOAT64) expenditure_pension_adjusted,
SAFE_CAST(expenditure_sickness_disability_adjusted AS FLOAT64) expenditure_sickness_disability_adjusted,
SAFE_CAST(expenditure_unemployment_benefit_adjusted AS FLOAT64) expenditure_unemployment_benefit_adjusted,
SAFE_CAST(expenditure_family_children_adjusted AS FLOAT64) expenditure_family_children_adjusted,
SAFE_CAST(expenditure_subsidies_adjusted AS FLOAT64) expenditure_subsidies_adjusted,
SAFE_CAST(expenditure_public_investment_adjusted AS FLOAT64) expenditure_public_investment_adjusted,
SAFE_CAST(expenditure_other_primary_expenditure_adjusted AS FLOAT64) expenditure_other_primary_expenditure_adjusted,
SAFE_CAST(expenditure_property_income_adjusted AS FLOAT64) expenditure_property_income_adjusted,
SAFE_CAST(expenditure_property_income_excep_interest_adjusted AS FLOAT64) expenditure_property_income_excep_interest_adjusted,
SAFE_CAST(current_receipt AS FLOAT64) current_receipt,
SAFE_CAST(current_receipt_except_interest AS FLOAT64) current_receipt_except_interest,
SAFE_CAST(current_receipt_adjusted AS FLOAT64) current_receipt_adjusted,
SAFE_CAST(total_receipt AS FLOAT64) total_receipt,
SAFE_CAST(current_expenditure AS FLOAT64) current_expenditure,
SAFE_CAST(current_expenditure_except_interest AS FLOAT64) current_expenditure_except_interest,
SAFE_CAST(current_expenditure_adjusted AS FLOAT64) current_expenditure_adjusted,
SAFE_CAST(current_expenditure_except_interest_adjusted AS FLOAT64) current_expenditure_except_interest_adjusted,
SAFE_CAST(total_expenditure AS FLOAT64) total_expenditure,
SAFE_CAST(net_lending AS FLOAT64) net_lending,
SAFE_CAST(primary_balance AS FLOAT64) primary_balance,
SAFE_CAST(net_lending_adjusted AS FLOAT64) net_lending_adjusted,
SAFE_CAST(primary_balance_adjusted AS FLOAT64) primary_balance_adjusted,
SAFE_CAST(underlying_net_lending AS FLOAT64) underlying_net_lending,
SAFE_CAST(underlying_primary_balance AS FLOAT64) underlying_primary_balance,
SAFE_CAST(net_financial_liabilities AS FLOAT64) net_financial_liabilities,
SAFE_CAST(financial_assets AS FLOAT64) financial_assets,
SAFE_CAST(gross_interest_paid AS FLOAT64) gross_interest_paid,
SAFE_CAST(gross_interest_received AS FLOAT64) gross_interest_received,
SAFE_CAST(net_interest_paid AS FLOAT64) net_interest_paid,
SAFE_CAST(gdp_current_prices AS FLOAT64) gdp_current_prices,
SAFE_CAST(gdp_volume AS FLOAT64) gdp_volume,
SAFE_CAST(gdp_potential_current_prices AS FLOAT64) gdp_potential_current_prices,
SAFE_CAST(gdp_potential_volume AS FLOAT64) gdp_potential_volume,
SAFE_CAST(output_gap AS FLOAT64) output_gap,
SAFE_CAST(short_term_interest_rate AS FLOAT64) short_term_interest_rate,
SAFE_CAST(long_term_interest_rate AS FLOAT64) long_term_interest_rate,
SAFE_CAST(index_cpi AS FLOAT64) index_cpi,
SAFE_CAST(exchange_rate AS FLOAT64) exchange_rate,
SAFE_CAST(effective_exchange_rate AS FLOAT64) effective_exchange_rate,
SAFE_CAST(real_effective_exchange_rate AS FLOAT64) real_effective_exchange_rate,
SAFE_CAST(total_employment AS FLOAT64) total_employment,
SAFE_CAST(government_employment AS FLOAT64) government_employment,
SAFE_CAST(labor_force AS FLOAT64) labor_force,
SAFE_CAST(unemployment_rate AS FLOAT64) unemployment_rate,
SAFE_CAST(export AS FLOAT64) export,
SAFE_CAST(import AS FLOAT64) import,
SAFE_CAST(deflator_export AS FLOAT64) deflator_export,
SAFE_CAST(deflator_import AS FLOAT64) deflator_import,
SAFE_CAST(deflator_gdp AS FLOAT64) deflator_gdp,
SAFE_CAST(government_fixed_capital_formation AS FLOAT64) government_fixed_capital_formation,
SAFE_CAST(capital_transfers AS FLOAT64) capital_transfers,
SAFE_CAST(government_consumption_fixed_capital AS FLOAT64) government_consumption_fixed_capital,
SAFE_CAST(capital_tax_transfers_receipts AS FLOAT64) capital_tax_transfers_receipts,
SAFE_CAST(term_trade AS FLOAT64) term_trade,
SAFE_CAST(trade_openness_ratio AS FLOAT64) trade_openness_ratio,
SAFE_CAST(primary_total_expenditure_adjustred AS FLOAT64) primary_total_expenditure_adjustred,
SAFE_CAST(total_expenditure_adjusted AS FLOAT64) total_expenditure_adjusted,
SAFE_CAST(total_receipt_adjusted AS FLOAT64) total_receipt_adjusted,
SAFE_CAST(primary_total_receipt_adjusted AS FLOAT64) primary_total_receipt_adjusted,
SAFE_CAST(expenditure_labor_policy_active AS FLOAT64) expenditure_labor_policy_active,
SAFE_CAST(expenditure_labor_policy_passive AS FLOAT64) expenditure_labor_policy_passive,
SAFE_CAST(size_municipalities AS FLOAT64) size_municipalities,
SAFE_CAST(share_women_parliament AS FLOAT64) share_women_parliament,
SAFE_CAST(share_women_minister AS FLOAT64) share_women_minister,
SAFE_CAST(government_confidence AS FLOAT64) government_confidence,
SAFE_CAST(rule_of_law_limited_power AS FLOAT64) rule_of_law_limited_power,
SAFE_CAST(rule_of_law_rights AS FLOAT64) rule_of_law_rights,
SAFE_CAST(expenditure_health_pc AS FLOAT64) expenditure_health_pc,
SAFE_CAST(judicial_confidence AS FLOAT64) judicial_confidence,
SAFE_CAST(rule_of_law_justice_enforcement AS FLOAT64) rule_of_law_justice_enforcement,
SAFE_CAST(rule_of_law_justice_government_influence AS FLOAT64) rule_of_law_justice_government_influence,
SAFE_CAST(index_ourdata AS FLOAT64) index_ourdata,
SAFE_CAST(internet_interaction_authoriries AS FLOAT64) internet_interaction_authoriries,
SAFE_CAST(average_income_tax_rate AS FLOAT64) average_income_tax_rate,
SAFE_CAST(average_employee_social_security_rate AS FLOAT64) average_employee_social_security_rate,
SAFE_CAST(average_employer_social_security_rate AS FLOAT64) average_employer_social_security_rate,
SAFE_CAST(average_income_social_security_rate AS FLOAT64) average_income_social_security_rate,
SAFE_CAST(net_personal_average_tax_rate AS FLOAT64) net_personal_average_tax_rate,
SAFE_CAST(average_tax_wedge AS FLOAT64) average_tax_wedge,
SAFE_CAST(marginal_tax_wedge AS FLOAT64) marginal_tax_wedge,
SAFE_CAST(total_red_expenditure_intramural AS FLOAT64) total_red_expenditure_intramural,
SAFE_CAST(total_red_expenditure_government AS FLOAT64) total_red_expenditure_government,
SAFE_CAST(budget_aproppriation_red AS FLOAT64) budget_aproppriation_red,
SAFE_CAST(basic_red_expenditure_intramural AS FLOAT64) basic_red_expenditure_intramural,
SAFE_CAST(basic_red_expenditure_government AS FLOAT64) basic_red_expenditure_government,
SAFE_CAST(female_labor_participation_rate AS FLOAT64) female_labor_participation_rate,
SAFE_CAST(male_labor_participation_rate AS FLOAT64) male_labor_participation_rate,
SAFE_CAST(fertility_rate AS FLOAT64) fertility_rate,
SAFE_CAST(life_expectancy AS FLOAT64) life_expectancy,
SAFE_CAST(gini_disposable_income AS FLOAT64) gini_disposable_income,
SAFE_CAST(gini_market_income AS FLOAT64) gini_market_income,
SAFE_CAST(gini_government_income AS FLOAT64) gini_government_income,
SAFE_CAST(poverty_rate AS FLOAT64) poverty_rate,
SAFE_CAST(pmr_market_regulation_indicator AS FLOAT64) pmr_market_regulation_indicator,
SAFE_CAST(pmr_state_control AS FLOAT64) pmr_state_control,
SAFE_CAST(pmr_barriers_entrepeneurship AS FLOAT64) pmr_barriers_entrepeneurship,
SAFE_CAST(pmr_barriers_trade_investment AS FLOAT64) pmr_barriers_trade_investment,
SAFE_CAST(employment_contract_protect_ex_collective_dismissal AS FLOAT64) employment_contract_protect_ex_collective_dismissal,
SAFE_CAST(employment_contract_protect_in_collective_dismissal AS FLOAT64) employment_contract_protect_in_collective_dismissal,
SAFE_CAST(cabinet_right AS FLOAT64) cabinet_right,
SAFE_CAST(cabinet_center AS FLOAT64) cabinet_center,
SAFE_CAST(cabinet_left AS FLOAT64) cabinet_left,
SAFE_CAST(cabinet_composition AS FLOAT64) cabinet_composition,
SAFE_CAST(cabinet_ideological_composition AS FLOAT64) cabinet_ideological_composition,
SAFE_CAST(cabinet_ideological_gap AS FLOAT64) cabinet_ideological_gap,
SAFE_CAST(government_change AS FLOAT64) government_change,
SAFE_CAST(election_turnout AS FLOAT64) election_turnout,
SAFE_CAST(budget_perspective_medium term AS FLOAT64) budget_perspective_medium term,
SAFE_CAST(performance_budget AS FLOAT64) performance_budget,
SAFE_CAST(government_capital_stock AS FLOAT64) government_capital_stock,
SAFE_CAST(ppp_capital_stock AS FLOAT64) ppp_capital_stock,
SAFE_CAST(corporate_income_tax_rate AS FLOAT64) corporate_income_tax_rate,
SAFE_CAST(vat_rate AS FLOAT64) vat_rate,
SAFE_CAST(voice_accountability AS FLOAT64) voice_accountability,
SAFE_CAST(regulatory_quality AS FLOAT64) regulatory_quality,
SAFE_CAST(rule_of_law AS FLOAT64) rule_of_law,
SAFE_CAST(political_stability AS FLOAT64) political_stability,
SAFE_CAST(government_effectiveness AS FLOAT64) government_effectiveness,
SAFE_CAST(corruption_control AS FLOAT64) corruption_control,
SAFE_CAST(indicator_fiscal_rule_expenditure AS INT64) indicator_fiscal_rule_expenditure,
SAFE_CAST(indicator_fiscal_rule_revenue AS INT64) indicator_fiscal_rule_revenue,
SAFE_CAST(indicator_fiscal_rule_balance AS INT64) indicator_fiscal_rule_balance,
SAFE_CAST(indicator_fiscal_rule_debt AS INT64) indicator_fiscal_rule_debt,
SAFE_CAST(indicator_fiscal_council AS INT64) indicator_fiscal_council
FROM basedosdados-dev.world_oecd_public_finance_staging.country_data AS t