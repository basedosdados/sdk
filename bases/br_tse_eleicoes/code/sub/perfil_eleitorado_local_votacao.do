
//----------------------------------------------------------------------------//
// build: perfil eleitorado - local votacao
//----------------------------------------------------------------------------//

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) case(preserve)
keep id_municipio id_municipio_tse
tempfile diretorio
save `diretorio'

//------------------------//
// loops
//------------------------//

foreach ano of numlist 2010(2)2024 {
	
	cap import delimited "input/perfil_eleitorado_local_votacao/eleitorado_local_votacao_`ano'/eleitorado_local_votacao_`ano'.txt", delim(";") stringcols(_all) varn(nonames) clear
	cap import delimited "input/perfil_eleitorado_local_votacao/eleitorado_local_votacao_`ano'/eleitorado_local_votacao_`ano'.csv", delim(";") stringcols(_all) varn(nonames) clear
	
	drop in 1
	
	keep v3 v6 v7 v8 v10 v11 v12 v15 v16 v17 v19 v20 v21 v22 v23 v24 v25 v27 v29 v31 v33 v35
	
	ren v3	ano
	ren v6	turno
	ren v7	sigla_uf
	ren v8	id_municipio_tse
	ren v10	zona
	ren v11	secao
	ren v12	tipo_secao_agregada
	ren v15	numero
	ren v16	nome
	ren v17	tipo
	ren v19	endereco
	ren v20	bairro
	ren v21	cep
	ren v22	telefone
	ren v23	latitude
	ren v24	longitude
	ren v25	situacao
	ren v27	situacao_zona
	ren v29	situacao_secao
	ren v31	situacao_localidade
	ren v33 situacao_secao_acessibilidade
	ren v35	eleitores_secao
	
	replace turno = "1" if turno == "1º Turno"
	replace turno = "2" if turno == "2º Turno"
	
	replace telefone = ""	if telefone == "-1"
	replace latitude = ""	if latitude == "-1"
	replace longitude = ""	if longitude == "-1"
	
	/*
	clean_string tipo_secao_agregada
	clean_string tipo
	clean_string situacao
	clean_string situacao_zona
	clean_string situacao_secao
	clean_string situacao_localidade
	clean_string situacao_secao_acessibilidade
	*/
	destring ano turno id_municipio_tse latitude longitude ///
		eleitores_secao, replace
	
	merge m:1 id_municipio_tse using `diretorio'
	drop if _merge == 2
	drop _merge
	order id_municipio, b(id_municipio_tse)
	
	order ano turno sigla_uf id_municipio id_municipio_tse zona secao
	
	tempfile perfil`ano'
	save `perfil`ano''
	
}
*

use `perfil2010'
foreach ano of numlist 2012(2)2024 {
	append using `perfil`ano''
}

order situacao_secao_acessibilidade, a(situacao_localidade)

compress

save "output/perfil_eleitorado_local_votacao.dta", replace
