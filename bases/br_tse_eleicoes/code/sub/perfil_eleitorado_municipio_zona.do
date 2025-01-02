
//----------------------------------------------------------------------------//
// build: perfil eleitorado
//----------------------------------------------------------------------------//

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) case(preserve)
keep id_municipio id_municipio_tse
tempfile diretorio
save `diretorio'

//------------------------//
// loops
//------------------------//

foreach ano of numlist 1994(2)2024 {
	
	cap import delimited "input/perfil_eleitorado/perfil_eleitorado_`ano'/perfil_eleitorado_`ano'.txt", delim(";") stringcols(_all) varn(nonames) clear //rowr(1:100000)
	cap import delimited "input/perfil_eleitorado/perfil_eleitorado_`ano'/perfil_eleitorado_`ano'.csv", delim(";") stringcols(_all) varn(nonames) clear //rowr(1:100000)
	
	drop in 1
	
	if `ano' <= 2022 {
		
		keep v3 v4 v5 v7 v9 v10 v12 v14 v16 v18 v19 v20 v21
		
		ren v3 ano
		ren v4 sigla_uf
		ren v5 id_municipio_tse
		ren v7 situacao_biometria
		ren v9 zona
		ren v10 genero
		ren v12 estado_civil
		ren v14 grupo_idade
		ren v16 instrucao
		ren v18 eleitores
		ren v19 eleitores_biometria
		ren v20 eleitores_deficiencia
		ren v21 eleitores_inclusao_nome_social
		
	}
	else if `ano' == 2024 {
		
		keep v3 v4 v5 v7 v8 v10 v12 v14 v24 v25 v26 v27
		
		ren v3 ano
		ren v4 sigla_uf
		ren v5 id_municipio_tse
		ren v7 zona
		ren v8 genero
		ren v10 estado_civil
		ren v12 grupo_idade
		ren v14 instrucao
		ren v24 eleitores
		ren v25 eleitores_biometria
		ren v26 eleitores_deficiencia
		ren v27 eleitores_inclusao_nome_social
		
		gen situacao_biometria = ""
		
		
	}
	
	
	destring ano id_municipio_tse zona eleitores*, replace force
	
	foreach k of varlist zona eleitores* {
		
		replace `k' = . if `k' == -1
		
	}
	*
	
	merge m:1 id_municipio_tse using `diretorio'
	drop if _merge == 2
	drop _merge
	order id_municipio, b(id_municipio_tse)
				
	replace ano = 2014 if ano == 201407
	
	order ano sigla_uf id_municipio id_municipio_tse situacao_biometria zona genero estado_civil grupo_idade instrucao ///
		eleitores eleitores_biometria eleitores_deficiencia eleitores_inclusao_nome_social
	
	compress
	
	save "output/perfil_eleitorado_municipio_zona_`ano'.dta", replace

}
*

