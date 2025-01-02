
//----------------------------------------------------------------------------//
// build: detalhes votacao
//----------------------------------------------------------------------------//

//------------------------//
// listas de estados
//------------------------//

local ufs_1933	               BR
local ufs_1934	               BR
local ufs_1945	   AL AM    BA    CE DF ES    GO    MA MG    MT PA PB PE PI PR    RJ RN       RS SC SE SP
local ufs_1947	   AL AM AP BA    CE DF ES    GO GP MA MG    MT PA PB PE PI PR RB RJ RN       RS SC SE SP
local ufs_1950	AC AL AM AP BA    CE DF ES    GO GP MA MG    MT PA PB PE PI PR RB RJ RN       RS SC SE SP
local ufs_1954	AC AL AM AP BA    CE DF ES    GO GP MA MG    MT PA PB PE PI PR RB RJ RN       RS SC SE SP
local ufs_1955	               BR
local ufs_1958	AC AL AM AP BA    CE DF ES    GO    MA MG    MT PA PB PE PI PR RB RJ RN RO    RS SC SE SP
local ufs_1960	               BR
local ufs_1962	AC AL AM AP BA    CE    ES GB GO    MA MG    MT PA PB PE PI PR    RJ RN RO    RS SC SE SP
local ufs_1965	               BR
local ufs_1966	AC AL AM AP BA    CE    ES GB GO    MA MG    MT PA PB PE PI PR    RJ RN RO RR RS SC SE SP
local ufs_1970	AC AL AM AP BA    CE    ES GB GO    MA MG    MT PA PB PE PI PR    RJ RN RO RR RS SC SE SP
local ufs_1974	AC AL AM AP BA    CE    ES GB GO    MA MG    MT PA PB PE PI PR    RJ RN RO RR RS SC SE SP
local ufs_1978	AC AL AM AP BA    CE    ES    GO    MA MG MS MT PA PB PE PI PR    RJ RN RO RR RS SC SE SP
local ufs_1982	AC AL AM AP BA    CE    ES    GO    MA MG MS MT PA PB PE PI PR    RJ RN RO RR RS SC SE SP
local ufs_1986	AC AL AM AP BA    CE DF ES    GO    MA MG MS MT PA PB PE PI PR    RJ RN RO RR RS SC SE SP
local ufs_1989	               BR
local ufs_1990	AC AL AM AP BA    CE DF ES    GO    MA MG MS MT PA PB PE PI PR    RJ RN RO RR RS SC SE SP TO

//------------------------//
// loops
//------------------------//

foreach ano of numlist 1945 1947 1955(5)1965 1950(4)1990 1989 { // 1933 1934
	
	foreach uf in `ufs_`ano'' {
		
		cap import delimited using "input/detalhe_votacao_uf/DETALHE_VOTACAO_UF_`ano'/DETALHE_VOTACAO_UF_`ano'_`uf'.txt", delimiter(";") varn(nonames) stringcols(_all) clear
		cap import delimited using "input/detalhe_votacao_uf/DETALHE_VOTACAO_UF_`ano'/DETALHE_VOTACAO_UF_`ano'.txt",      delimiter(";") varn(nonames) stringcols(_all) clear
		
		if `ano' <= 2012 {
			
			keep v3 v4 v5 v6 v9 v10 v11 v12 v13 v14 v15 v16  v17 v18 v19 v20 v21 v22
			
			ren v3 ano
			ren v4 turno
			ren v5 tipo_eleicao
			ren v6 sigla_uf
			ren v9 cargo
			ren v10 aptos
			ren v11 comparecimento
			ren v12 abstencoes
			ren v13 votos_validos
			ren v14 votos_brancos
			ren v15 votos_nulos
			ren v16 votos_legenda
			ren v17 votos_anulados_apurado_separado
			ren v18 secoes_totalizadas
			ren v19 secoes_anuladas
			ren v20 secoes_sem_funcionamento
			ren v21 zonas_eleitorais
			ren v22 juntas_apuradoras
			
			/*
			QTD_VOTOS_ANULADOS_APU_SEP
			QTD_SECOES_TOT
			QTD_SECOES_ANULADAS
			QTD_SECOES_SEM_FUNCION
			QTD_ZONAS_ELEITORAIS
			QTD_JUNTAS_APURADORAS
			*/
			
			//ren v11 secoes
			//ren v15 secoes_agregadas
			//ren v16 aptos_totalizadas
			//ren v17 secoes_totalizadas
			
			
		}
		*
		
		destring ano turno aptos* secoes* comparecimento abstencoes votos_* zonas* juntas*, replace force
		
		foreach k of varlist aptos* secoes* comparecimento abstencoes votos_* zonas* juntas* {
			
			replace `k' = . if `k' == -1
			replace `k' = . if `k' == -3
			
		}
		*
		
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
		
		gen proporcao_comparecimento = 100 * comparecimento / aptos
		la var proporcao_comparecimento "% Comparecimento"
		
		gen proporcao_votos_validos = 100 * votos_validos / comparecimento
		la var proporcao_votos_validos "% Votos Válidos"
		
		gen proporcao_votos_brancos = 100 * votos_brancos / comparecimento
		la var proporcao_votos_brancos "% Votos Brancos"
		
		gen proporcao_votos_nulos = 100 * votos_nulos / comparecimento
		la var proporcao_votos_nulos "% Votos Nulos"
		
		duplicates drop
		
		compress
		
		tempfile detalhes_`ano'_`uf'
		save `detalhes_`ano'_`uf''
		
	}
	*
	
	//------------------//
	// append
	//------------------//
	
	local first: word 1 of `ufs_`ano''
	use `detalhes_`ano'_`first'', clear
	foreach uf in `ufs_`ano'' {
		if "`uf'" != "`first'" {
			append using `detalhes_`ano'_`uf''
		}
	}
	
	order	ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf cargo ///
			secoes_anuladas secoes_sem_funcionamento zonas_eleitorais juntas_apuradoras votos_anulados_apurado_separado secoes_totalizadas ///
			aptos comparecimento abstencoes votos_validos votos_brancos votos_nulos votos_legenda ///
			proporcao_*
	
	compress
	
	save "output/detalhes_votacao_uf_`ano'.dta", replace
	
}
*
