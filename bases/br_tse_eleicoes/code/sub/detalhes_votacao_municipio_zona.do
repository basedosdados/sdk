
//----------------------------------------------------------------------------//
// build: detalhes votacao
//----------------------------------------------------------------------------//

//------------------------//
// listas de estados
//------------------------//

local estados_1994	AC AL AM AP BA BRASIL          GO MA    MS             PI             RR RS SC SE SP TO
local estados_1996	AC AL AM AP BA        CE    ES GO MA MG MS    PA PB    PI       RN    RR       SE SP TO
local estados_1998	AC AL AM AP BA BRASIL CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2000	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2002	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2004	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2006	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2008	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2010	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2012	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2014	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2016	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2018	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2020	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2022	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local estados_2024	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO

//------------------------//
// loops
//------------------------//

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) encoding("utf-8") //stringcols(_all)
keep id_municipio id_municipio_tse
tempfile municipio
save `municipio'

foreach ano of numlist 1994(2)2024 {
	
	foreach estado in `estados_`ano'' {
		
		cap import delimited using "input/detalhe_votacao_munzona/detalhe_votacao_munzona_`ano'/detalhe_votacao_munzona_`ano'_`estado'.txt", delimiter(";") varn(nonames) stringcols(_all) clear
		cap import delimited using "input/detalhe_votacao_munzona/detalhe_votacao_munzona_`ano'/detalhe_votacao_munzona_`ano'_`estado'.csv", delimiter(";") varn(nonames) stringcols(_all) clear
		
		//br if v14 == "01120" & v18 == "PRESIDENTE"
		
		if `ano' == 1994 | `ano' == 1998 {
			
			drop in 1
			
			keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v19 v20 v21 v24 v26 ///
				v31 v32 ///
				v41 ///
				v43 v44 v45
			
			ren v3  ano
			ren v6  turno
			ren v7  id_eleicao
			ren v8  tipo_eleicao
			ren v9  data_eleicao
			ren v11 sigla_uf
			ren v14 id_municipio_tse
			ren v16 zona
			ren v18 cargo
			ren v19 aptos
			ren v20 secoes
			ren v21 secoes_agregadas
			ren v24 comparecimento
			ren v26 abstencoes
			ren v31 votos_nominais // validos
			ren v32 votos_legenda // validos
			ren v41 votos_brancos
			
			destring votos_nominais votos_legenda v43 v44 v45, replace
			
			gen votos_validos = votos_nominais + votos_legenda
			gen votos_nulos = v43 + v44 + v45
			
			drop v43 v44 v45
			
			gen aptos_totalizadas = .
			gen secoes_totalizadas = .
		
		}
		else if `ano' == 1996 {
			
			drop in 1
			
			keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v19 v20 v21 v22 v23 v24 v25 v27 v28 v29 v30
			
			ren v3  ano
			ren v6  turno
			ren v7  id_eleicao
			ren v8  tipo_eleicao
			ren v9  data_eleicao
			ren v11 sigla_uf
			ren v14 id_municipio_tse
			ren v16 zona
			ren v18 cargo
			ren v19 aptos
			ren v20 secoes
			ren v21 secoes_agregadas
			ren v22 aptos_totalizadas
			ren v23 secoes_totalizadas
			ren v24 comparecimento
			ren v25 abstencoes
			ren v27 votos_nominais
			ren v28 votos_brancos
			ren v29 votos_nulos
			ren v30 votos_legenda
			
			destring votos_nominais votos_legenda, replace
			
			gen votos_validos = votos_nominais + votos_legenda
			
		}
		else if `ano' >= 2000 & `ano' <= 2016  {
			
			drop in 1
			
			keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v19 v20 v21 v22 v23 v24 v25 v27 v28 v29 v30
			
			ren v3  ano
			ren v6  turno
			ren v7  id_eleicao
			ren v8  tipo_eleicao
			ren v9  data_eleicao
			ren v11 sigla_uf
			ren v14 id_municipio_tse
			ren v16 zona
			ren v18 cargo
			ren v19 aptos
			ren v20 secoes
			ren v21 secoes_agregadas
			ren v22 aptos_totalizadas
			ren v23 secoes_totalizadas
			ren v24 comparecimento
			ren v25 abstencoes
			ren v27 votos_nominais
			ren v28 votos_brancos
			ren v29 votos_nulos
			ren v30 votos_legenda
			
			destring votos_nominais votos_legenda, replace
			
			gen votos_validos = votos_nominais + votos_legenda
			
		}
		else if `ano' >= 2018 {
			
			drop in 1
			
			keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v19 v20 v21 v24 v26 v30 v31 v41 v42 v32
			
			ren v3  ano
			ren v6  turno
			ren v7  id_eleicao
			ren v8  tipo_eleicao
			ren v9  data_eleicao
			ren v11 sigla_uf
			ren v14 id_municipio_tse
			ren v16 zona
			ren v18 cargo
			ren v19 aptos
			ren v20 secoes
			ren v21 secoes_agregadas
			ren v24 comparecimento
			ren v26 abstencoes
			ren v30 votos_validos
			ren v31 votos_nominais
			ren v41 votos_brancos
			ren v42 votos_nulos
			ren v32 votos_legenda
			
			gen aptos_totalizadas = .
			gen secoes_totalizadas = .
			
		}
		*
		
		destring ano turno id_municipio_tse zona aptos* secoes* comparecimento abstencoes votos_*, replace force
		
		foreach k of varlist zona aptos* secoes* comparecimento abstencoes votos_* {
			
			replace `k' = . if `k' == -1
			
		}
		*
		
		if "`estado'" == "BRASIL" {
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
		
		// erro dados TSE vindo com valores diferentes para identificadores duplicados
		//duplicates tag ano turno tipo_eleicao sigla_uf id_municipio_tse zona cargo, gen(d)
		//sort d ano turno tipo_eleicao sigla_uf id_municipio_tse zona cargo
		collapse (sum) aptos* secoes* comparecimento abstencoes votos*, by(ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio_tse zona cargo)
		
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
		
		gen proporcao_votos_validos = 100 * votos_validos / comparecimento
		la var proporcao_votos_validos "% Votos Válidos"
		
		gen proporcao_votos_brancos = 100 * votos_brancos / comparecimento
		la var proporcao_votos_brancos "% Votos Brancos"
		
		gen proporcao_votos_nulos = 100 * votos_nulos / comparecimento
		la var proporcao_votos_nulos "% Votos Nulos"
		
		drop if mod(ano, 2) > 0
		
		compress
		
		tempfile detalhes_`ano'_`estado'
		save `detalhes_`ano'_`estado''
		
	}
	*
	
	//------------------//
	// append
	//------------------//
	
	use `detalhes_`ano'_AC', clear
	foreach estado in `estados_`ano'' {
		if "`estado'" != "AC" {
			append using `detalhes_`ano'_`estado''
		}
	}
	*
	
	order	ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio id_municipio_tse zona cargo aptos secoes secoes_agregadas aptos_totalizadas secoes_totalizadas ///
			comparecimento abstencoes votos_validos votos_brancos votos_nulos votos_nominais votos_legenda ///
			proporcao_*
	
	duplicates drop
	
	duplicates tag ano turno tipo_eleicao sigla_uf id_municipio_tse zona cargo, gen(dup)
	sort dup       ano turno tipo_eleicao sigla_uf id_municipio_tse zona cargo
	//drop if dup > 0 & comparecimento == 0
	drop dup
	
	compress
	
	save "output/detalhes_votacao_municipio_zona_`ano'.dta", replace
	
}
*
