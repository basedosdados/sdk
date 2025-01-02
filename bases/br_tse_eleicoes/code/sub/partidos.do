
//----------------------------------------------------------------------------//
// build: partidos
//----------------------------------------------------------------------------//

//------------------------//
// lista de estados
//------------------------//

local ufs_1990	AC AL AM AP BA    CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_1994	AC AL AM AP BA BR          GO MA    MS                            RR RS SC SE SP TO
local ufs_1996	AC AL AM AP BA    CE    ES GO MA MG MS    PA PB PE PI       RN    RR RS    SE SP TO
local ufs_1998	AC AL AM AP BA    CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2000	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2002	AC AL AM AP BA    CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2004	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2006	AC AL AM AP BA    CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2008	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2010	AC AL AM AP BA    CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
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

foreach ano of numlist 1990 1994(2)2024 {
	
	foreach uf in `ufs_`ano'' {
		
		cap import delimited "input/consulta_coligacao/CONSULTA_LEGENDA_`ano'/CONSULTA_LEGENDA_`ano'_`uf'.txt",     delim(";") varn(nonames) stringcols(_all) clear
		cap import delimited "input/consulta_coligacao/consulta_legendas_`ano'/CONSULTA_LEGENDA_`ano'_`uf'.txt",    delim(";") varn(nonames) stringcols(_all) clear
		cap import delimited "input/consulta_coligacao/consulta_legendas_`ano'/consulta_legendas_`ano'_`uf'.txt",   delim(";") varn(nonames) stringcols(_all) clear
		cap import delimited "input/consulta_coligacao/consulta_legendas_`ano'/consulta_legendas_`ano'_`uf'.csv",   delim(";") varn(nonames) stringcols(_all) clear
		cap import delimited "input/consulta_coligacao/consulta_coligacao_`ano'/consulta_coligacao_`ano'_`uf'.txt", delim(";") varn(nonames) stringcols(_all) clear
		cap import delimited "input/consulta_coligacao/consulta_coligacao_`ano'/consulta_coligacao_`ano'_`uf'.csv", delim(";") varn(nonames) stringcols(_all) clear
		
		if `ano' <= 2012 {
			
			if mod(`ano', 4) == 2 {
				keep v3 v4 v5 v7 v10 v11 v12 v13 v14 v16 v17 v18
				ren v7 sigla_uf
			}
			else {
				keep v3 v4 v5 v6 v7 v10 v11 v12 v13 v14 v16 v17 v18
				ren v6 sigla_uf
				ren v7 id_municipio_tse
			}
			
			ren v3	ano
			ren v4	turno
			ren v5	tipo_eleicao
			ren v10	cargo
			ren v11	tipo_agremiacao
			ren v12	numero
			ren v13	sigla
			ren v14	nome
			ren v16	nome_coligacao
			ren v17 composicao_coligacao
			ren v18	sequencial_coligacao
			
			gen id_eleicao = ""
			gen data_eleicao = ""
			
		}
		else if `ano' >= 2014 & `ano' <= 2020 {
			
			drop in 1
			
			if mod(`ano', 4) == 2 {
				keep v3 v6 v7 v8 v9 v11 v14 v15 v16 v17 v18 v19 v20 v21
				ren v11 sigla_uf
			}
			else {
				keep v3 v6 v7 v8 v9 v10 v11 v14 v15 v16 v17 v18 v19 v20 v21
				ren v10 sigla_uf
				ren v11 id_municipio_tse
			}
			
			ren v3	ano
			ren v6	turno
			ren v7	id_eleicao
			ren v8	tipo_eleicao
			ren v9	data_eleicao
			ren v14	cargo
			ren v15	tipo_agremiacao
			ren v16	numero
			ren v17	sigla
			ren v18	nome
			ren v19	sequencial_coligacao
			ren v20	nome_coligacao
			ren v21 composicao_coligacao
			
		}
		else if `ano' >= 2022 {
			
			drop in 1
			
			if mod(`ano', 4) == 2 {
				keep v3 v6 v7 v8 v9 v11 v14 v15 v16 v17 v18 v19 v20 v21 v22 v23 v24 v25 v27
				ren v11 sigla_uf
			}
			else {
				keep v3 v6 v7 v8 v9 v10 v11 v14 v15 v16 v17 v18 v19 v20 v21 v22 v23 v24 v25 v27
				ren v10 sigla_uf
				ren v11 id_municipio_tse
			}
			
			ren v3	ano
			ren v6	turno
			ren v7	id_eleicao
			ren v8	tipo_eleicao
			ren v9	data_eleicao
			ren v14	cargo
			ren v15	tipo_agremiacao
			ren v16	numero
			ren v17	sigla
			ren v18	nome
			ren v19 numero_federacao
			ren v20 nome_federacacao
			ren v21 sigla_federacao
			ren v22 composicao_federacao
			ren v23	sequencial_coligacao
			ren v24	nome_coligacao
			ren v25 composicao_coligacao
			ren v27 situacao_legenda
			
		}
		*
		
		replace sigla_uf = "" if sigla_uf == "BR"
		cap gen id_municipio_tse = ""
		
		destring ano turno id_municipio_tse, replace force
		
		merge m:1 id_municipio_tse using `diretorio'
		drop if _merge == 2
		drop _merge
		order id_municipio id_municipio_tse, a(sigla_uf)
		
		//------------------//
		// limpa strings
		//------------------//
		
		replace nome = "PARTIDO HUMANISTA DA SOLIDARIEDADE" if nome == "PARTIDO DA HUMANISTA DA SOLIDARIEDADE"
		
		replace sequencial_coligacao = "" if inlist(sequencial_coligacao, "-3", "-1")
		if `ano' >= 2022 {
			replace numero_federacao = "" if inlist(numero_federacao, "-3", "-1")
		}
		
		foreach k in nome_coligacao nome_federacacao sigla_federacao composicao_federacao nome_coligacao composicao_coligacao {
			cap replace `k' = "" if `k' == "#NE#" | `k' == "#NULO#"
		}
		
		foreach k in tipo_eleicao cargo tipo_agremiacao situacao_legenda {
			cap clean_string `k'
		}
		
		limpa_tipo_eleicao	`ano'
		limpa_partido		`ano' sigla
		
		//------------------//
		// salva
		//------------------//
		
		order tipo_agremiacao, a(nome)
		order ano turno tipo_eleicao
		
		tempfile partidos_`uf'_`ano'
		save `partidos_`uf'_`ano''
		
	}
	*
	
	//------------------//
	// append
	//------------------//
	
	use `partidos_AC_`ano'', clear
	foreach uf in `ufs_`ano'' {
		if "`uf'" != "AC" {
			append using `partidos_`uf'_`ano''
		}
	}
	*
	
	//------------------//
	// duplicadas
	//------------------//
	
	duplicates drop
	
	//duplicates tag ano turno tipo_eleicao sigla_uf id_municipio_tse cargo numero, gen(dup)
	//drop if dup > 0 & strpos(nome_coligacao, "¿") > 0
	//drop dup
	
	gen aux = (tipo_agremiacao == "coligacao")
	bys ano turno tipo_eleicao sigla_uf id_municipio_tse cargo numero: egen aux_coligacao = max(aux)
	duplicates tag ano turno tipo_eleicao sigla_uf id_municipio_tse cargo numero, gen(dup)
	drop if dup > 0 & aux_coligacao == 1 & tipo_agremiacao == "partido isolado" // assumimos que quando há coligacao reportada, essa é a verdade
	drop dup aux*
	
	duplicates drop ano turno id_eleicao sigla_uf id_municipio_tse cargo numero, force
		// se sobram duplicadas, pegamos um aleatorio (perdendo sequencial_coligacao, nome_coligacao, composicao_coligacao)
	
	order ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio_tse cargo
	
	compress
	
	save "output/partidos_`ano'.dta", replace
	
}
*
