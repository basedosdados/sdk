
//----------------------------------------------------------------------------//
// build: filiacao partidaria
//----------------------------------------------------------------------------//

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) encoding("utf-8")
keep id_municipio id_municipio_tse
tempfile diretorio
save `diretorio'

local partidos	avante cidadania dc dem mdb novo patriota pcb pcdob pco pdt phs pl pmb ///
				pmn pode pp ppl pros prp prtb psb psc psd psdb psl psol pstu pt ///
				ptb ptc pv rede republicanos solidariedade up

foreach partido in `partidos' {
	
	local filename "input/filiacao_partidaria/filiados_`partido'/filiados_`partido'.csv"
	capture confirm file `filename'
	
	if _rc == 0 {
	
		import delimited `filename', clear varn(nonames) delim(";") stringc(_all)
		
		drop in 1
		
		keep v3 v4 v5 v7 v8 v10 v11 v12 v13 v14 v15 v16 v17 v18 v19
		
		ren v3	titulo_eleitoral
		ren v4	nome
		ren v5	sigla_partido
		ren v7	sigla_uf
		ren v8	id_municipio_tse
		ren v10	zona
		ren v11	secao
		ren v12	data_filiacao
		ren v13	situacao_registro
		ren v14	tipo_registro
		ren v15	data_processamento
		ren v16	data_desfiliacao
		ren v17	data_cancelamento
		ren v18	data_regularizacao
		ren v19	motivo_cancelamento
		
		destring id_municipio_tse zona secao, replace force
		
		merge m:1 id_municipio_tse using `diretorio'
		drop if _merge == 2
		drop _merge
		order id_municipio, b(id_municipio_tse)
		
		foreach k of varlist data_* {
			
			replace `k' = substr(`k', 7, 4) + "-" + substr(`k', 4, 2) + "-" + substr(`k', 1, 2) if `k' != ""
			
		}
		*
		
		foreach k in nome {
			cap replace `k' = ustrtitle(`k')
		}
		*
		
		order sigla_uf id_municipio id_municipio_tse zona secao titulo_eleitoral nome
		
		tempfile filiacao_`partido'
		save `filiacao_`partido''
		
	}
}

use `filiacao_avante', clear
foreach partido in `partidos' {
	if !("`partido'" == "avante") { //"`estado'" == "ac" & 
		cap append using `filiacao_`partido''
	}
}
*

sort sigla_uf id_municipio zona secao data_filiacao

compress

save "output/filiacao_partidaria.dta", replace


