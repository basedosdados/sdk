
//----------------------------------------------------------------------------//
// build: candidatos
//----------------------------------------------------------------------------//

//------------------------//
// lista de ufs
//------------------------//

local ufs_1994	AC AL AM AP BA          GO MA    MS                         RO RR RS SC SE SP TO
local ufs_1996	AC AL AM AP BA CE    ES GO MA MG MS    PA PB PE PI       RN    RR RS    SE SP TO
local ufs_1998	AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2000	AC AL AM AP BA CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2002	AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2004	AC AL AM AP BA CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2006	AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2008	AC AL AM AP BA CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2010	AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2012	AC AL AM AP BA CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2014	AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2016	AC AL AM AP BA CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2018	AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2020	AC AL AM AP BA CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2022	AC AL AM AP BA CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2024	AC AL AM AP BA CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO

//------------------------//
// loops
//------------------------//

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) encoding("utf-8")
keep id_municipio id_municipio_tse
tempfile diretorio
save `diretorio'

foreach ano of numlist 1994(2)2024 {
	
	foreach uf in `ufs_`ano'' {
		
		di "`ano'_`uf'_candidatos"
		
		cap import delimited using "input/consulta_vagas/consulta_vagas_`ano'/consulta_vagas_`ano'_`uf'.txt", delim(";") varn(nonames) stringcols(_all) clear
		cap import delimited using "input/consulta_vagas/consulta_vagas_`ano'/consulta_vagas_`ano'_`uf'.csv", delim(";") varn(nonames) stringcols(_all) clear
		
		if `ano' <= 2012 {

			keep v3 v4 v5 v6 v9 v10
			
			ren v3	ano
			ren v4	tipo_eleicao
			ren v5	sigla_uf
			ren v6	id_municipio_tse
			ren v9	cargo
			ren v10	vagas
			
			gen id_eleicao = ""
			gen data_eleicao = ""
		
		}
		else if `ano' >= 2014 {
			
			drop in 1
			
			keep v3 v6 v7 v8 v10 v11 v14 v15
			
			ren v3	ano
			ren v6  id_eleicao
			ren v7	tipo_eleicao
			ren v8  data_eleicao
			ren v10	sigla_uf
			ren v11	id_municipio_tse
			ren v14	cargo
			ren v15	vagas
			
		}
		*
		
		destring ano id_municipio_tse vagas, replace force
		
		//------------------//
		// limpa strings
		//------------------//
		
		foreach k in tipo_eleicao cargo {
			cap clean_string `k'
		}
		*
		
		limpa_tipo_eleicao `ano'
		
		foreach k in eleicao {
			replace data_`k' = substr(data_`k', 7, 4) + "-" + substr(data_`k', 4, 2) + "-" + substr(data_`k', 1, 2) if data_`k' != ""
			replace data_`k' = "" if real(substr(data_`k', 1, 4)) < 1900
		}
		
		drop if mod(ano, 2) > 0
		
		merge m:1 id_municipio_tse using `diretorio'
		drop if _merge == 2
		drop _merge
		order id_municipio, b(id_municipio_tse)
		
		duplicates drop
		
		tempfile vagas_`uf'_`ano'
		save `vagas_`uf'_`ano''
		
	}
	*
	
	//------------------//
	// append
	//------------------//
	
	use `vagas_AC_`ano'', clear
	foreach uf in `ufs_`ano'' {
		if "`uf'" != "AC" {
			append using `vagas_`uf'_`ano''
		}
	}
	*
	
	order ano id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio id_municipio_tse cargo
	
	compress
	
	save "output/vagas_`ano'.dta", replace
	
}
*
