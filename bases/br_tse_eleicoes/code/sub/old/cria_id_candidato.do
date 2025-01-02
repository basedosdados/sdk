
//----------------------------------------------------------------------------//
// build: id_candidato
//----------------------------------------------------------------------------//

use "output/candidatos_1998.dta", clear
foreach ano of numlist 2000(2)2022 {
	append using "output/candidatos_`ano'.dta"
}
*

//------------------------------//
// limpa entradas erradas
//------------------------------//

drop if cpf == "" | cpf == "#NULO#" | cpf == "000000000-4" | cpf == "0" | cpf == "-4" | cpf == "00000000000" | cpf == "NR_CPF_CANDIDATO"
drop if titulo_eleitoral == "000000000000" | titulo_eleitoral == "#NI#"

//------------------------------//
// rodada 1
// usa cpf and titulo_eleitoral
// mantém só linhas identificadas unicamente
//------------------------------//

keep nome cpf titulo_eleitoral
duplicates drop

egen id_candidato_bd = group(cpf titulo_eleitoral), missing
la var id_candidato_bd "ID Candidato(a) - Base dos Dados"

bys cpf: egen aux_id_number_cpf = max(id_candidato_bd)
replace id_candidato_bd = aux_id_number_cpf

bys titulo_eleitoral: egen aux_id_number_TE = max(id_candidato_bd)
replace id_candidato_bd = aux_id_number_TE

tostring id_candidato_bd, replace
gen id_candidato_bd_str = "1 " + id_candidato_bd
destring id_candidato_bd, replace

egen aux_cpf = tag(id_candidato_bd cpf)
bys id_candidato_bd: egen N_cpf = sum(aux_cpf)

egen aux_TE = tag(id_candidato_bd titulo_eleitoral)
bys id_candidato_bd: egen N_TE = sum(aux_TE)

drop aux*

preserve
	keep if N_cpf > 1 | N_TE > 1
	save "tmp/para_2a_rodada.dta", replace
restore

keep if N_cpf == 1 & N_TE == 1

order id_candidato_bd_str cpf titulo_eleitoral nome
keep  id_candidato_bd_str cpf titulo_eleitoral nome

save "tmp/1a_rodada.dta", replace

//------------------------------//
// rodada 2
// separar por nomes
//------------------------------//

use "tmp/para_2a_rodada.dta", clear

gen nome_limpo = nome
clean_string nome_limpo

gen aux_primeira = word(nome_limpo, 1)
gen aux_ultima   = word(nome_limpo, -1)

// consertando strings erradas identificadas no olho
replace aux_primeira = "elves"		if aux_primeira == "elvis" & aux_ultima == "leite"
replace aux_primeira = "lourival"	if aux_primeira == "luciano" & aux_ultima == "dombroski"
replace aux_primeira = "vanderley"	if aux_primeira == "vanderlley" & aux_ultima == "passos"
replace aux_primeira = "manoel"		if aux_primeira == "manuel" & aux_ultima == "santos"
replace aux_primeira = "daizymar"	if aux_primeira == "deizymar" & aux_ultima == "messini"
replace aux_primeira = "moacir"		if aux_primeira == "moacy" & aux_ultima == "evangelista"

replace aux_ultima = "pontim"		if aux_primeira == "antonio" & aux_ultima == "pontin"
replace aux_ultima = "messine"		if aux_primeira == "daizymar" & aux_ultima == "messini"
replace aux_ultima = "ortigosa"		if aux_primeira == "ideraldo" & aux_ultima == "ortigossa"
replace aux_ultima = "rezende"		if aux_primeira == "cecilio" & aux_ultima == "resende"
replace aux_ultima = "souza"		if aux_primeira == "antonio" & aux_ultima == "sousa"
replace aux_ultima = "paula"		if aux_primeira == "nuncia" & aux_ultima == "souza"
replace aux_ultima = "silva"		if aux_primeira == "custodia" & aux_ultima == "sessim"
replace aux_ultima = "soares"		if aux_primeira == "rosangela" & aux_ultima == "dantas"

egen aux_nomes = tag(id_candidato_bd_str aux_primeira aux_ultima)
bys id_candidato_bd_str: egen N_nomes = sum(aux_nomes)
drop aux_nomes

preserve
	keep if N_nomes > 1
	save "tmp/para_3a_rodada.dta", replace
restore

keep if N_nomes == 1	// excluindo candidatos nao-identificados unicamente

egen aux_id = group(id_candidato_bd_str aux_primeira aux_ultima)
tostring aux_id, replace
replace id_candidato_bd_str = "2 " + aux_id
drop aux_id

keep id_candidato_bd_str cpf titulo_eleitoral nome

save "tmp/2a_rodada.dta", replace

/*
COMENTADO POIS AINDA GERA CASOS ERRADOS COM MESMAS INICIAIS MAS NOMES MUITO DIFERENTES
ESCOLHENDO SER MAIS CONSERVADOR PARA (TENTAR) GARANTIR UNICIDADE VERDADEIRA NOS IDs

//------------------------------//
// rodada 3
// mesmas iniciais
//------------------------------//

use "tmp/para_3a_rodada.dta", clear

gen aux_inicial_primeira = substr(aux_primeira, 1, 1)
gen aux_inicial_ultima   = substr(aux_ultima, 1, 1)

egen aux_tag_inicial_primeira = tag(id_candidato_bd aux_inicial_primeira)
egen aux_tag_inicial_ultima   = tag(id_candidato_bd aux_inicial_ultima)

bys id_candidato_bd_str: egen N_iniciais_primeira = sum(aux_tag_inicial_primeira)
bys id_candidato_bd_str: egen N_iniciais_ultima   = sum(aux_tag_inicial_ultima)

preserve
	keep if N_iniciais_primeira > 1 | N_iniciais_ultima > 1
	save "tmp/para_4a_rodada.dta", replace
restore

keep if N_iniciais_primeira == 1 & N_iniciais_ultima == 1

keep id_candidato_bd_str cpf titulo_eleitoral nome

save "tmp/3a_rodada.dta", replace
*/


//------------------------------//
// append
//------------------------------//

use "tmp/1a_rodada.dta", clear
append using "tmp/2a_rodada.dta"

egen id_candidato_bd = group(id_candidato_bd_str)
la var id_candidato_bd "ID Candidato - Base dos Dados"

sort  id_candidato_bd
order id_candidato_bd cpf titulo_eleitoral nome
keep  id_candidato_bd cpf titulo_eleitoral nome

ren id_candidato_bd id_candidato_bd_v1

compress

save "output/id_candidato_bd_v1.dta", replace




//----------------------------------------------------------------------------//
// build: id_candidato
//----------------------------------------------------------------------------//

use "output/candidatos_1998.dta", clear
foreach ano of numlist 2000(2)2024 {
	append using "output/candidatos_`ano'.dta"
}
*

//------------------------------//
// limpa entradas erradas
//------------------------------//

//drop if cpf == "" | cpf == "#NULO#" | cpf == "000000000-4" | cpf == "0" | cpf == "-4" | cpf == "00000000000" | cpf == "NR_CPF_CANDIDATO"
drop if titulo_eleitoral == "000000000000" | titulo_eleitoral == "#NI#"

//------------------------------//
// rodada 1
// usa cpf and titulo_eleitoral
// mantém só linhas identificadas unicamente
//------------------------------//

keep nome titulo_eleitoral // cpf
duplicates drop

egen id_candidato_bd = group(titulo_eleitoral), missing // cpf
la var id_candidato_bd "ID Candidato - Base dos Dados"

//bys cpf: egen aux_id_number_cpf = max(id_candidato_bd)
//replace id_candidato_bd = aux_id_number_cpf

bys titulo_eleitoral: egen aux_id_number_TE = max(id_candidato_bd)
replace id_candidato_bd = aux_id_number_TE

tostring id_candidato_bd, replace
gen id_candidato_bd_str = "1 " + id_candidato_bd
destring id_candidato_bd, replace

//egen aux_cpf = tag(id_candidato_bd cpf)
//bys id_candidato_bd: egen N_cpf = sum(aux_cpf)

egen aux_TE = tag(id_candidato_bd titulo_eleitoral)
bys id_candidato_bd: egen N_TE = sum(aux_TE)

drop aux*

preserve
	keep if N_TE > 1 // N_cpf > 1 | 
	save "tmp/para_2a_rodada.dta", replace
restore

keep if N_TE == 1 // N_cpf == 1 & 

order id_candidato_bd_str titulo_eleitoral nome // cpf
keep  id_candidato_bd_str titulo_eleitoral nome // cpf

save "tmp/1a_rodada.dta", replace

//------------------------------//
// rodada 2
// separar por nomes
//------------------------------//

use "tmp/para_2a_rodada.dta", clear

if _N > 0 {
	
	gen nome_limpo = nome
	clean_string nome_limpo
	
	gen aux_primeira = word(nome_limpo, 1)
	gen aux_ultima   = word(nome_limpo, -1)
	
	// consertando strings erradas identificadas no olho
	replace aux_primeira = "elves"		if aux_primeira == "elvis" & aux_ultima == "leite"
	replace aux_primeira = "lourival"	if aux_primeira == "luciano" & aux_ultima == "dombroski"
	replace aux_primeira = "vanderley"	if aux_primeira == "vanderlley" & aux_ultima == "passos"
	replace aux_primeira = "manoel"		if aux_primeira == "manuel" & aux_ultima == "santos"
	replace aux_primeira = "daizymar"	if aux_primeira == "deizymar" & aux_ultima == "messini"
	replace aux_primeira = "moacir"		if aux_primeira == "moacy" & aux_ultima == "evangelista"
	
	replace aux_ultima = "pontim"		if aux_primeira == "antonio" & aux_ultima == "pontin"
	replace aux_ultima = "messine"		if aux_primeira == "daizymar" & aux_ultima == "messini"
	replace aux_ultima = "ortigosa"		if aux_primeira == "ideraldo" & aux_ultima == "ortigossa"
	replace aux_ultima = "rezende"		if aux_primeira == "cecilio" & aux_ultima == "resende"
	replace aux_ultima = "souza"		if aux_primeira == "antonio" & aux_ultima == "sousa"
	replace aux_ultima = "paula"		if aux_primeira == "nuncia" & aux_ultima == "souza"
	replace aux_ultima = "silva"		if aux_primeira == "custodia" & aux_ultima == "sessim"
	replace aux_ultima = "soares"		if aux_primeira == "rosangela" & aux_ultima == "dantas"
	
	egen aux_nomes = tag(id_candidato_bd_str aux_primeira aux_ultima)
	bys id_candidato_bd_str: egen N_nomes = sum(aux_nomes)
	drop aux_nomes
	
	preserve
		keep if N_nomes > 1
		save "tmp/para_3a_rodada.dta", replace
	restore
	
	keep if N_nomes == 1	// excluindo candidatos nao-identificados unicamente
	
	egen aux_id = group(id_candidato_bd_str aux_primeira aux_ultima)
	tostring aux_id, replace
	replace id_candidato_bd_str = "2 " + aux_id
	drop aux_id
	
}

keep id_candidato_bd_str titulo_eleitoral nome // cpf

save "tmp/2a_rodada.dta", replace

/*
COMENTADO POIS AINDA GERA CASOS ERRADOS COM MESMAS INICIAIS MAS NOMES MUITO DIFERENTES
ESCOLHENDO SER MAIS CONSERVADOR PARA (TENTAR) GARANTIR UNICIDADE VERDADEIRA NOS IDs

//------------------------------//
// rodada 3
// mesmas iniciais
//------------------------------//

use "tmp/para_3a_rodada.dta", clear

gen aux_inicial_primeira = substr(aux_primeira, 1, 1)
gen aux_inicial_ultima   = substr(aux_ultima, 1, 1)

egen aux_tag_inicial_primeira = tag(id_candidato_bd aux_inicial_primeira)
egen aux_tag_inicial_ultima   = tag(id_candidato_bd aux_inicial_ultima)

bys id_candidato_bd_str: egen N_iniciais_primeira = sum(aux_tag_inicial_primeira)
bys id_candidato_bd_str: egen N_iniciais_ultima   = sum(aux_tag_inicial_ultima)

preserve
	keep if N_iniciais_primeira > 1 | N_iniciais_ultima > 1
	save "tmp/para_4a_rodada.dta", replace
restore

keep if N_iniciais_primeira == 1 & N_iniciais_ultima == 1

keep id_candidato_bd_str cpf titulo_eleitoral nome

save "tmp/3a_rodada.dta", replace
*/


//------------------------------//
// append
//------------------------------//

use          "tmp/1a_rodada.dta", clear
append using "tmp/2a_rodada.dta"

egen id_candidato_bd = group(id_candidato_bd_str)
la var id_candidato_bd "ID Candidato - Base dos Dados"

sort  id_candidato_bd
order id_candidato_bd titulo_eleitoral nome // cpf
keep  id_candidato_bd titulo_eleitoral nome // cpf

compress

save "output/id_candidato_bd_v2.dta", replace


//------------------------------//
// combinações CPF-titulo eleitoral
//------------------------------//

use "output/candidatos_1998.dta", clear
foreach ano of numlist 2000(2)2024 {
	append using "output/candidatos_`ano'.dta"
}
*

keep cpf titulo_eleitoral

drop if cpf == "" | titulo_eleitoral == ""
duplicates drop

foreach k in cpf titulo_eleitoral {
	duplicates tag `k', gen(dup)
	drop if dup > 0 & `k' != ""
	drop dup
}

save "tmp/cpf_titulo_eleitoral.dta", replace





