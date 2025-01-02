
//----------------------------------------------------------------------------//
// build: resultados secao
//----------------------------------------------------------------------------//

//------------------------//
// listas de estados
//------------------------//

local ufs_1994	AC AL AM AP BA BR          GO MA                   PI          RO    RS SC SE SP TO
local ufs_1996	AC AL AM AP BA    CE    ES GO MA MG MS    PA PB PE PI       RN    RR RS    SE SP TO
local ufs_1998	AC AL AM AP BA BR CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2000	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP   
local ufs_2002	AC AL AM AP BA BR CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2004	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2006	AC AL AM AP BA BR CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2008	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2010	AC AL AM AP BA BR CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2012	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2014	AC AL AM AP BA BR CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2016	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2018	AC AL AM AP BA BR CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO	
local ufs_2020	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2022	AC AL AM AP BA BR CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2024	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO

//------------------------//
// loops
//------------------------//

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) case(preserve)
keep id_municipio id_municipio_tse
tempfile diretorio
save `diretorio'

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) case(preserve)
keep id_municipio_tse sigla_uf
tempfile diretorio_ufs
save `diretorio_ufs'

foreach ano of numlist 1994(2)2024 {
	
	foreach uf in `ufs_`ano'' {
		
		di "`ano'_`uf'"
		
		cap import delimited "input/votacao_secao/votacao_secao_`ano'_`uf'/votacao_secao_`ano'_`uf'.txt", delim(";") varn(nonames) stringcols(_all) clear
		cap import delimited "input/votacao_secao/votacao_secao_`ano'_`uf'/votacao_secao_`ano'_`uf'.csv", delim(";") varn(nonames) stringcols(_all) clear
		
		if (`ano' == 1998 & "`uf'" == "BR") | (`ano' == 2008 & "`uf'" == "TO") { // schema antigo, nao foi atualizado no site do TSE
			
			keep v3 v4 v5 v6 v8 v10 v11 v13 v14 v15
			
			ren v3	ano
			ren v4	turno
			ren v5	tipo_eleicao
			ren v6	sigla_uf
			ren v8	id_municipio_tse
			ren v10	zona
			ren v11	secao
			ren v13	cargo
			ren v14	numero_votavel
			ren v15	votos
			
			gen id_eleicao = ""
			gen data_eleicao = ""
			
		}
		else {
			
			drop in 1
			
			keep v3 v6 v7 v8 v9 v11 v14 v16 v17 v19 v20 v22
			
			ren v3	ano
			ren v6	turno
			ren v7  id_eleicao
			ren v8	tipo_eleicao
			ren v9  data_eleicao
			ren v11	sigla_uf
			ren v14	id_municipio_tse
			ren v16	zona
			ren v17	secao
			ren v19	cargo
			ren v20	numero_votavel
			ren v22	votos
			
		}
		
		destring ano turno id_municipio_tse votos, replace force // remover zeros a esquerda
		
		//------------------//
		// limpa strings
		//------------------//
		
		if inlist(`ano', 1994, 1998) & "`uf'" == "BR" {
			drop sigla_uf
			merge m:1 id_municipio_tse using `diretorio_ufs', keep(1 3) nogenerate
			replace sigla_uf = "ZZ" if sigla_uf == ""
		}
		
		foreach k in tipo_eleicao cargo {
			clean_string `k'
		}
		*
		
		foreach k in eleicao {
			replace data_`k' = substr(data_`k', 7, 4) + "-" + substr(data_`k', 4, 2) + "-" + substr(data_`k', 1, 2) if data_`k' != ""
			replace data_`k' = "" if real(substr(data_`k', 1, 4)) < 1900
		}
			
		limpa_tipo_eleicao `ano'
		
		cap limpa_candidato
		
		merge m:1 id_municipio_tse using `diretorio', keep(1 3) nogenerate

		order id_municipio, b(id_municipio_tse)
		
		//tempfile resultados_secao_`uf'_`ano'
		//save `resultados_secao_`uf'_`ano''
		save "tmp/resultados_secao_`uf'.dta", replace
		
		//------------------------------------//
		// separa votos candidato vs partido
		// para seguir output original do tse
		// a nivel municipio-zona
		//------------------------------------//
		
		//-------------//
		// candidato
		//-------------//
		
		//use `resultados_secao_`uf'_`ano''
		use "tmp/resultados_secao_`uf'.dta", clear
		
		drop if inlist(numero_votavel, "95", "96", "97")
		
		drop if length(numero_votavel) == 2 & ///
				inlist(cargo, "vereador", "deputado estadual", "deputado distrital", "deputado federal", "senador")
		
		ren numero_votavel numero_candidato
		
		//tempfile resultados_cand_secao_`uf'_`ano'
		//save `resultados_cand_secao_`uf'_`ano''
		save "tmp/resultados_cand_secao_`uf'.dta", replace
		
		//-------------//
		// partido
		//-------------//
		
		//use `resultados_secao_`uf'_`ano'', clear
		use "tmp/resultados_secao_`uf'.dta", clear
		
		drop if inlist(numero_votavel, "95", "96", "97")
		
		preserve
			
			gen numero_partido = substr(numero_votavel, 1, 2)
			
			drop if length(numero_votavel) == 2 & ///
					inlist(cargo, "vereador", "deputado estadual", "deputado distrital", "deputado federal", "senador")
			
			collapse (sum) votos, by(ano id_eleicao tipo_eleicao data_eleicao turno sigla_uf id_municipio id_municipio_tse zona secao cargo numero_partido)
			
			ren votos votos_nominais
			
			//tempfile votos_nominais
			//save `votos_nominais'
			save "tmp/votos_nominais.dta", replace
			
		restore
		preserve
			
			keep if length(numero_votavel) == 2 & ///
					inlist(cargo, "vereador", "deputado estadual", "deputado distrital", "deputado federal", "senador")
			
			ren numero_votavel	numero_partido
			ren votos			votos_legenda
			
			//tempfile votos_legenda
			//save `votos_legenda'
			save "tmp/votos_legenda.dta", replace
			
		restore
		
		use "tmp/votos_nominais.dta", clear
		
		merge 1:1 ano id_eleicao turno sigla_uf id_municipio_tse zona secao cargo numero_partido using "tmp/votos_legenda.dta", nogenerate
		
		replace votos_nominais  = 0	if votos_nominais == .
		replace votos_legenda 	= 0	if votos_legenda  == .
		
		//tempfile resultados_part_secao_`uf'_`ano'
		//save `resultados_part_secao_`uf'_`ano''
		save "tmp/resultados_part_secao_`uf'.dta", replace
		
		erase "tmp/resultados_secao_`uf'.dta"
		erase "tmp/votos_nominais.dta"
		erase "tmp/votos_legenda.dta"
		
	}
	*
	
	//-------------------//
	// append candidato
	//-------------------//
	
	use "tmp/resultados_cand_secao_AC.dta", clear
	foreach uf in `ufs_`ano'' {
		if "`uf'" != "AC" {
			append using "tmp/resultados_cand_secao_`uf'.dta"
		}
	}
	*
	
	duplicates drop ano turno            tipo_eleicao                                    id_municipio_tse zona secao cargo numero_candidato, force // poucos casos finais com duplicadas
	local vars      ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio id_municipio_tse zona secao cargo numero_candidato
	
	order `vars'
	sort  `vars'
	
	compress
	
	save "output/resultados_candidato_secao_`ano'.dta", replace
	
	//-------------------//
	// append partido
	//-------------------//
	
	use "tmp/resultados_part_secao_AC.dta", clear
	foreach uf in `ufs_`ano'' {
		if "`uf'" != "AC" {
			append using "tmp/resultados_part_secao_`uf'.dta"
		}
	}
	*
	
	local vars ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio id_municipio_tse zona secao cargo numero_partido
	
	order `vars'
	sort  `vars'
	
	compress
	
	save "output/resultados_partido_secao_`ano'.dta", replace
	
	foreach uf in `ufs_`ano'' {
		erase "tmp/resultados_cand_secao_`uf'.dta"
		erase "tmp/resultados_part_secao_`uf'.dta"
	}
	
}
*
