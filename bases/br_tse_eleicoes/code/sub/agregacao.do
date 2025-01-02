
//----------------------------------------------------------------------------//
// build: agregacao
//----------------------------------------------------------------------------//

//---------------------------------//
// resultados
//---------------------------------//

//---------------------//
// candidato-municipio
//---------------------//

!mkdir "output/resultados_candidato_municipio"

foreach ano of numlist 1994(2)2024 {
	
	!mkdir "output/resultados_candidato_municipio/ano=`ano'"
	
	local subdirs : dir "output/resultados_candidato_municipio_zona/ano=`ano'/" dirs "*"
	
	foreach subdir in `subdirs' {
		
		local sigla_uf = substr("`subdir'", 10, 2)
		!mkdir "output/resultados_candidato_municipio/ano=`ano'/sigla_uf=`sigla_uf'"
		
		import delimited "output/resultados_candidato_municipio_zona/ano=`ano'/sigla_uf=`sigla_uf'/resultados_candidato_municipio_zona.csv", ///
			clear varn(1) encoding("utf-8") case(preserve) stringcols(_all)
		
		destring votos, replace
		collapse (sum) votos, by(turno id_eleicao tipo_eleicao data_eleicao id_municipio id_municipio_tse cargo numero_partido sigla_partido titulo_eleitoral_candidato sequencial_candidato numero_candidato resultado)
		
		export delimited "output/resultados_candidato_municipio/ano=`ano'/sigla_uf=`sigla_uf'/resultados_candidato_municipio.csv", replace
		
	}
}
*

//---------------------//
// partido-municipio
//---------------------//

!mkdir "output/resultados_partido_municipio"

foreach ano of numlist 1994(2)2024 {
	
	!mkdir "output/resultados_partido_municipio/ano=`ano'"
	
	local subdirs : dir "output/resultados_partido_municipio_zona/ano=`ano'/" dirs "*"
	
	foreach subdir in `subdirs' {
		
		local sigla_uf = substr("`subdir'", 10, 2)
		!mkdir "output/resultados_partido_municipio/ano=`ano'/sigla_uf=`sigla_uf'"
		
		import delimited "output/resultados_partido_municipio_zona/ano=`ano'/sigla_uf=`sigla_uf'/resultados_partido_municipio_zona.csv", clear varn(1) encoding("utf-8") case(preserve)
		
		collapse (sum) votos*, by(turno id_eleicao tipo_eleicao data_eleicao id_municipio id_municipio_tse cargo numero_partido sigla_partido)
		
		export delimited "output/resultados_partido_municipio/ano=`ano'/sigla_uf=`sigla_uf'/resultados_partido_municipio.csv", replace
		
	}
}
*

//---------------------//
// candidato
//---------------------//

use "output/norm_candidatos.dta", clear
keep if mod(ano, 4) == 0
keep ano tipo_eleicao sigla_uf id_municipio_tse cargo titulo_eleitoral numero numero_partido sigla_partido
tostring numero numero_partido, replace
tempfile candidatos_mod0
save `candidatos_mod0'

use "output/norm_candidatos.dta", clear
keep if mod(ano, 4) == 2 & cargo != "presidente"
keep ano tipo_eleicao sigla_uf cargo titulo_eleitoral numero numero_partido sigla_partido
tostring numero numero_partido, replace
tempfile candidatos_mod2_estadual
save `candidatos_mod2_estadual'

use "output/norm_candidatos.dta", clear
keep if mod(ano, 4) == 2 & cargo == "presidente"
keep ano tipo_eleicao cargo titulo_eleitoral numero numero_partido sigla_partido
tostring numero numero_partido, replace
tempfile candidatos_mod2_presid
save `candidatos_mod2_presid'

!mkdir "output/resultados_candidato"

foreach ano of numlist 1945 1947 1955(5)1965 1950(4)1990 1989 {
	
	!mkdir "output/resultados_candidato/ano=`ano'"
	
	use "output/resultados_candidato_uf_`ano'.dta", clear
	
	replace sigla_uf = "" if cargo == "presidente"
	gen id_municipio = .
	gen id_municipio_tse = .
	
	cap gen id_eleicao = ""
	cap gen data_eleicao = ""
	cap gen numero_partido = ""
	cap gen titulo_eleitoral_candidato = ""
	cap gen sequencial_candidato = ""
	cap gen numero_candidato = ""
	
	collapse (sum) votos, by(turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio id_municipio_tse cargo numero_partido sigla_partido titulo_eleitoral_candidato sequencial_candidato numero_candidato nome_candidato resultado)
	order                    turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio id_municipio_tse cargo numero_partido sigla_partido titulo_eleitoral_candidato sequencial_candidato numero_candidato nome_candidato resultado votos
	
	duplicates tag turno tipo_eleicao sigla_uf id_municipio_tse cargo sequencial_candidato numero_candidato nome_candidato, gen(dup)
	drop if dup > 0  // TODO: fazer limpeza mais cuidadosa, deletando linhas específicas
	drop dup
	
	export delimited "output/resultados_candidato/ano=`ano'/resultados_candidato.csv", replace
	
}

foreach ano of numlist 1994(2)2024 {
	
	!mkdir "output/resultados_candidato/ano=`ano'"
	
	use "output/resultados_candidato_municipio_zona_`ano'.dta", clear
	
	cap ren sequencial       sequencial_candidato
	cap ren numero           numero_candidato
	cap ren nome             nome_candidato
	cap ren nome_urna        nome_urna_candidato
	
	ren numero_candidato numero
	
	if mod(`ano', 4) == 0 {
		
		merge m:1 ano tipo_eleicao sigla_uf id_municipio_tse cargo numero using `candidatos_mod0'
		drop if _merge == 2
		drop _merge
		
	}
	else {
		
		preserve
			
			keep if cargo != "presidente"
			
			merge m:1 ano tipo_eleicao sigla_uf cargo numero using `candidatos_mod2_estadual'
			drop if _merge == 2
			drop _merge
			
			tempfile resultados_mod2_estadual
			save `resultados_mod2_estadual'
			
		restore
		preserve
			
			keep if cargo == "presidente"
			
			merge m:1 ano tipo_eleicao cargo numero using `candidatos_mod2_presid'
			drop if _merge == 2
			drop _merge
			
			tempfile resultados_mod2_presid
			save `resultados_mod2_presid'
			
		restore
		
		use `resultados_mod2_estadual', clear
		append using `resultados_mod2_presid'
		
	}
	*
	
	ren titulo_eleitoral titulo_eleitoral_candidato
	ren numero numero_candidato
	
	replace sigla_uf = ""			if cargo == "presidente"
	replace id_municipio = .		if mod(ano, 4) == 2
	replace id_municipio_tse = .	if mod(ano, 4) == 2
	
	collapse (sum) votos, by(turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio id_municipio_tse cargo numero_partido sigla_partido titulo_eleitoral_candidato sequencial_candidato numero_candidato nome_candidato resultado)
	order                    turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio id_municipio_tse cargo numero_partido sigla_partido titulo_eleitoral_candidato sequencial_candidato numero_candidato nome_candidato resultado votos
	
	export delimited "output/resultados_candidato/ano=`ano'/resultados_candidato.csv", replace
	
}
*

//---------------------------------//
// detalhes votacao municipio
//---------------------------------//

!mkdir "output/detalhes_votacao_municipio"

foreach ano of numlist 2024 { // 1994(2)2024 {
	
	!mkdir "output/detalhes_votacao_municipio/ano=`ano'"
	
	local subdirs : dir "output/detalhes_votacao_municipio_zona/ano=`ano'/" dirs "*"
	
	foreach subdir in `subdirs' {
		
		local sigla_uf = substr("`subdir'", 10, 2)
		!mkdir "output/detalhes_votacao_municipio/ano=`ano'/sigla_uf=`sigla_uf'"
		
		import delimited "output/detalhes_votacao_municipio_zona/ano=`ano'/sigla_uf=`sigla_uf'/detalhes_votacao_municipio_zona.csv", clear varn(1) case(preserve)
		
		collapse (sum) aptos secoes secoes_agregadas aptos_totalizadas secoes_totalizadas comparecimento abstencoes votos_*, ///
			by(turno id_eleicao tipo_eleicao data_eleicao id_municipio id_municipio_tse cargo)
		
		gen proporcao_comparecimento = 100 * comparecimento / aptos
		gen proporcao_votos_validos	 = 100 * votos_validos  / comparecimento
		gen proporcao_votos_brancos	 = 100 * votos_brancos  / comparecimento
		gen proporcao_votos_nulos	 = 100 * votos_nulos    / comparecimento
		
		export delimited "output/detalhes_votacao_municipio/ano=`ano'/sigla_uf=`sigla_uf'/detalhes_votacao_municipio.csv", replace
		
	}
}
*

