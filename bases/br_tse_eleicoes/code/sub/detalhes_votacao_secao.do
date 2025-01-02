
//----------------------------------------------------------------------------//
// build: detalhes votacao secao
//----------------------------------------------------------------------------//

//------------------------//
// listas de estados
//------------------------//

local ufs_1994	AC AL AM AP BA BRASIL          GO MA    MS             PI                RS SC SE SP TO
local ufs_1996	AC AL AM AP BA        CE    ES GO MA MG MS    PA PB PE PI       RN    RR RS    SE SP TO
local ufs_1998	AC AL AM AP BA BRASIL CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2000	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP 
local ufs_2002	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2004	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2006	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2008	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2010	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2012	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2014	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2016	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2018	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2020	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2022	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2024	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO

//------------------------//
// loops
//------------------------//

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) encoding("utf-8")
keep id_municipio id_municipio_tse
tempfile municipio
save `municipio'

foreach ano of numlist 1994(2)2024 {
	
	foreach uf in `ufs_`ano'' {
		
		di "Ano: `ano' - UF: `uf'"
		
		cap import delimited using "input/detalhe_votacao_secao/detalhe_votacao_secao_`ano'/detalhe_votacao_secao_`ano'_`uf'.txt", delimiter(";") varn(nonames) stringcols(_all) clear
		cap import delimited using "input/detalhe_votacao_secao/detalhe_votacao_secao_`ano'/detalhe_votacao_secao_`ano'_`uf'.csv", delimiter(";") varn(nonames) stringcols(_all) clear
		
		drop in 1
		
		keep v3 v6 v7 v8 v9 v11 v14 v16 v17 v19 v20 v21 v22 v23 v24 v25 v26 v27
		
		ren v3  ano
		ren v6  turno
		ren v7  id_eleicao
		ren v8  tipo_eleicao
		ren v9  data_eleicao
		ren v11 sigla_uf
		ren v14 id_municipio_tse
		ren v16 zona
		ren v17 secao
		ren v19 cargo
		ren v20 aptos
		ren v21 comparecimento
		ren v22 abstencoes
		ren v23 votos_nominais
		ren v24 votos_brancos
		ren v25 votos_nulos
		ren v26 votos_legenda
		ren v27 votos_nulos_apu_sep
		
		destring ano turno id_municipio_tse zona secao aptos* comparecimento abstencoes votos_*, replace force
		
		foreach k of varlist zona secao aptos* comparecimento abstencoes votos_* {
			
			replace `k' = . if `k' == -1
			
		}
		*
		
		if "`uf'" == "BRASIL" {
			keep if cargo == "PRESIDENTE" | sigla_uf == "DF"
		}
		
		//------------------//
		// limpa strings
		//------------------//
		
		foreach k in tipo_eleicao cargo {
			cap clean_string `k'
		}
		*
		
		limpa_tipo_eleicao `ano'
		
		//------------------//
		// variaveis
		//------------------//
		
		merge m:1 id_municipio_tse using `municipio'
		drop if _merge == 2
		drop _merge
		order id_municipio, b(id_municipio_tse)
		
		foreach k in eleicao {
			replace data_`k' = substr(data_`k', 7, 4) + "-" + substr(data_`k', 4, 2) + "-" + substr(data_`k', 1, 2)
			replace data_`k' = "" if real(substr(data_`k', 1, 4)) < 1900
		}
		
		gen proporcao_comparecimento = 100 * comparecimento / aptos
		la var proporcao_comparecimento "% Comparecimento"
		
		gen proporcao_votos_nominais = 100 * votos_nominais / comparecimento
		la var proporcao_votos_nominais "% Votos Nominais"
		
		gen proporcao_votos_legenda = 100 * votos_legenda / comparecimento
		la var proporcao_votos_legenda "% Votos em Legenda"
		
		gen proporcao_votos_brancos = 100 * votos_brancos / comparecimento
		la var proporcao_votos_brancos "% Votos Brancos"
		
		gen proporcao_votos_nulos = 100 * votos_nulos / comparecimento
		la var proporcao_votos_nulos "% Votos Nulos"
		
		drop if mod(ano, 2) > 0
		
		duplicates tag ano turno tipo_eleicao sigla_uf id_municipio_tse zona secao cargo, gen(dup) // idealmente usar o codigo_eleicao (v7) para identificar
		drop if dup > 0
		drop dup
		
		tempfile detalhes_secao_`ano'_`uf'
		save `detalhes_secao_`ano'_`uf''
		
	}
	*
	
	//------------------//
	// append
	//------------------//
	
	use `detalhes_secao_`ano'_AC', clear
	foreach uf in `ufs_`ano'' {
		if "`uf'" != "AC" {
			append using `detalhes_secao_`ano'_`uf''
		}
	}
	*
	
	order	ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio id_municipio_tse zona secao cargo aptos comparecimento abstencoes ///
			votos_nominais votos_brancos votos_nulos votos_legenda votos_nulos_apu_sep //votos_pendentes
	
	compress
	
	save "output/detalhes_votacao_secao_`ano'.dta", replace
	
}
*
