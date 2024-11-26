
//----------------------------------------------------------------------------//
// build: candidatos
//----------------------------------------------------------------------------//

//------------------------//
// lista de estados
//------------------------//

local ufs_1994	AC AL AM AP BA BR          GO MA    MS             PI             RR RS    SE SP TO
local ufs_1996	AC AL AM AP BA    CE    ES GO MA MG MS    PA PB PE PI       RN    RR RS    SE SP TO
local ufs_1998	AC AL AM AP BA BR CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2000	AC AL AM AP BA    CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
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

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) encoding("utf-8")
keep id_municipio id_municipio_tse
tempfile municipio
save `municipio'

foreach ano of numlist 1994(2)2024 {
	
	foreach uf in `ufs_`ano'' {
		
		di "`ano'_`uf'_candidatos"
		
		cap import delimited "input/consulta_cand/consulta_cand_`ano'/consulta_cand_`ano'_`uf'.txt", ///
			delim(";") varn(nonames) stripquotes(yes) bindquotes(nobind) stringcols(_all) clear
		cap import delimited "input/consulta_cand/consulta_cand_`ano'/consulta_cand_`ano'_`uf'.csv", ///
			delim(";") varn(nonames) stripquotes(yes) bindquotes(nobind) stringcols(_all) clear
		
		if `ano' <= 2014 | `ano' == 2018 {
			
			drop in 1
			
			keep v3 v6 v7 v8 v9 v11 v12 v15 v16 v17 v18 v19 v21 v22 v26 v28 v29 v35 v36 v38 v39 v41 v43 v45 v47 v49 v51 v54
			
			ren v3  ano
			ren v6  turno
			ren v7  id_eleicao
			ren v8  tipo_eleicao
			ren v9  data_eleicao
			ren v11 sigla_uf
			ren v12 id_municipio_tse
			ren v15 cargo
			ren v16 sequencial
			ren v17 numero
			ren v18 nome
			ren v19 nome_urna
			ren v21 cpf
			ren v22 email
			ren v26 situacao
			ren v28 numero_partido
			ren v29 sigla_partido
			ren v35 nacionalidade
			ren v36 sigla_uf_nascimento
			ren v38 municipio_nascimento
			ren v39 data_nascimento
			ren v41 titulo_eleitoral
			ren v43 genero
			ren v45 instrucao
			ren v47 estado_civil
			ren v49 raca
			ren v51 ocupacao
			ren v54 resultado
			
		}
		else if `ano' == 2016 | (`ano' >= 2020 & `ano' <= 2022) {
			
			drop in 1
			
			keep v3 v6 v7 v8 v9 v11 v12 v15 v16 v17 v18 v19 v21 v22 v26 v28 v29 v39 v40 v42 v43 v45 v47 v49 v51 v53 v55 v58
			
			ren v3  ano
			ren v6  turno
			ren v7  id_eleicao
			ren v8  tipo_eleicao
			ren v9  data_eleicao
			ren v11 sigla_uf
			ren v12 id_municipio_tse
			ren v15 cargo
			ren v16 sequencial
			ren v17 numero
			ren v18 nome
			ren v19 nome_urna
			ren v21 cpf
			ren v22 email
			ren v26 situacao
			ren v28 numero_partido
			ren v29 sigla_partido
			ren v39 nacionalidade
			ren v40 sigla_uf_nascimento
			ren v42 municipio_nascimento
			ren v43 data_nascimento
			ren v45 titulo_eleitoral
			ren v47 genero
			ren v49 instrucao
			ren v51 estado_civil
			ren v53 raca
			ren v55 ocupacao
			ren v58 resultado
			
		}
		else if `ano' == 2024 {
			
			drop in 1
			
			keep v3 v6 v7 v8 v9 v11 v12 v15 v16 v17 v18 v19 v21 v22 v26 v27 v36 v37 v38 v40 v42 v44 v46 v48 v50
			
			ren v3  ano
			ren v6  turno
			ren v7  id_eleicao
			ren v8  tipo_eleicao
			ren v9  data_eleicao
			ren v11 sigla_uf
			ren v12 id_municipio_tse
			ren v15 cargo
			ren v16 sequencial
			ren v17 numero
			ren v18 nome
			ren v19 nome_urna
			ren v21 cpf
			ren v22 email
			ren v26 numero_partido
			ren v27 sigla_partido
			ren v36 sigla_uf_nascimento
			ren v37 data_nascimento
			ren v38 titulo_eleitoral
			ren v40 genero
			ren v42 instrucao
			ren v44 estado_civil
			ren v46 raca
			ren v48 ocupacao
			ren v50 resultado
			
			tempfile basico
			save `basico'
			
			cap import delimited "input/consulta_cand/consulta_cand_complementar_`ano'/consulta_cand_complementar_`ano'_`uf'.csv", ///
				delim(";") varn(nonames) stripquotes(yes) bindquotes(nobind) stringcols(_all) clear
			
			drop in 1
			
			keep v4 v5 v9 v11 v28
			
			ren v4 id_eleicao
			ren v5 sequencial
			ren v9 nacionalidade
			ren v11 municipio_nascimento
			ren v28 situacao
			
			tempfile complementar
			save `complementar'
			
			use `basico'
			
			merge 1:1 id_eleicao sequencial using `complementar', nogenerate
			
			
		}
		
		//------------------//
		// limpa strings
		//------------------//
		
		destring ano turno id_municipio_tse, replace force
		
		merge m:1 id_municipio_tse using `municipio'
		drop if _merge == 2
		drop _merge
		order id_municipio, b(id_municipio_tse)
		
		foreach k of varlist _all {
			cap replace `k' = "" if ///
				`k' == "#NULO#" | ///
				`k' == "#NULO" | ///
				`k' == "#NE#" | ///
				`k' == "#NE" | ///
				`k' == "#NI#" | ///
				`k' == "##VERIFICAR BASE 1994##" | ///
				`k' == "-1" | ///
				`k' == "-4" | ///
				`k' == "NÃO DIVULGÁVEL" | ///
				`k' == "Não divulgável" | ///
				`k' == "Não Divulgável"
		}
		*
		
		foreach k in tipo_eleicao cargo nacionalidade genero instrucao estado_civil raca ocupacao situacao resultado {
			cap clean_string `k'
		}
		foreach k in nome nome_urna municipio_nascimento {
			cap replace `k' = ustrtitle(`k')
		}
		
		replace email = lower(email)
		
		limpa_tipo_eleicao	`ano'
		limpa_partido		`ano' sigla_partido
		limpa_candidato
		limpa_instrucao
		limpa_estado_civil
		limpa_resultado
		
		replace cpf = "0"      + cpf if length(cpf) == 10
		replace cpf = "00"     + cpf if length(cpf) == 9
		replace cpf = "000"    + cpf if length(cpf) == 8
		replace cpf = "0000"   + cpf if length(cpf) == 7
		replace cpf = "00000"  + cpf if length(cpf) == 6
		replace cpf = "000000" + cpf if length(cpf) == 5
		
		replace titulo_eleitoral = "" if titulo_eleitoral == "000000000000" | titulo_eleitoral == "0"
		
		replace titulo_eleitoral = "0"      + titulo_eleitoral if length(titulo_eleitoral) == 11
		replace titulo_eleitoral = "00"     + titulo_eleitoral if length(titulo_eleitoral) == 10
		replace titulo_eleitoral = "000"    + titulo_eleitoral if length(titulo_eleitoral) == 9
		replace titulo_eleitoral = "0000"   + titulo_eleitoral if length(titulo_eleitoral) == 8
		replace titulo_eleitoral = "00000"  + titulo_eleitoral if length(titulo_eleitoral) == 7
		replace titulo_eleitoral = "000000" + titulo_eleitoral if length(titulo_eleitoral) == 6
		
		replace titulo_eleitoral = "00000" + substr(titulo_eleitoral, 1, 5) + substr(titulo_eleitoral, 11, 2) if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 6,  5) == "     "
		replace titulo_eleitoral = "0000"  + substr(titulo_eleitoral, 1, 6) + substr(titulo_eleitoral, 11, 2) if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 7,  4) == "    "
		replace titulo_eleitoral = "000"   + substr(titulo_eleitoral, 1, 7) + substr(titulo_eleitoral, 11, 2) if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 8,  3) == "   "
		replace titulo_eleitoral = "00"    + substr(titulo_eleitoral, 1, 8) + substr(titulo_eleitoral, 11, 2) if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 9,  2) == "  "
		replace titulo_eleitoral = "0"     + substr(titulo_eleitoral, 1, 9) + substr(titulo_eleitoral, 11, 2) if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 10, 1) == " "
		
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 1)  + "0" + substr(titulo_eleitoral, 3,  10) if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 2,  1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 2)  + "0" + substr(titulo_eleitoral, 4,  9)  if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 3,  1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 3)  + "0" + substr(titulo_eleitoral, 5,  8)  if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 4,  1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 4)  + "0" + substr(titulo_eleitoral, 6,  7)  if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 5,  1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 5)  + "0" + substr(titulo_eleitoral, 7,  6)  if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 6,  1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 6)  + "0" + substr(titulo_eleitoral, 8,  5)  if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 7,  1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 7)  + "0" + substr(titulo_eleitoral, 9,  4)  if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 8,  1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 8)  + "0" + substr(titulo_eleitoral, 10, 3)  if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 9,  1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 9)  + "0" + substr(titulo_eleitoral, 11, 2)  if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 10, 1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 10) + "0" + substr(titulo_eleitoral, 12, 1)  if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 11, 1) == " "
		replace titulo_eleitoral = substr(titulo_eleitoral, 1, 11) + "0"                                    if length(titulo_eleitoral) == 12 & substr(titulo_eleitoral, 12, 1) == " "
		
		replace cargo = "vice-presidente" 		if cargo == "vice presidente"
		replace cargo = "vice-prefeito"   		if cargo == "vice prefeito"
		
		replace genero = "" 					if genero == "nao divulgavel" | genero == "nao informado"
		
		replace nacionalidade = "brasileira"	if nacionalidade == "brasileira nata"
		replace nacionalidade = ""				if nacionalidade == "nao divulgavel" | nacionalidade == "nao informado" | nacionalidade == "nao informada"
		
		replace sigla_uf_nascimento = ""        if sigla_uf_nascimento == " "
		
		cap replace raca = "" 					if raca == "sem informacao" | raca == "nao divulgavel" | raca == "nao informado" | raca == "nao informada"
		
		replace resultado = "" 					if inlist(resultado, "-1", "1", "4")
		
		replace sigla_uf = "" 					if cargo == "presidente" | cargo == "vice-presidente"
		
		//------------------//
		// limpa datas
		//------------------//
		
		foreach k in eleicao nascimento {
			replace data_`k' = substr(data_`k', 7, 4) + "-" + substr(data_`k', 4, 2) + "-" + substr(data_`k', 1, 2) if length(data_`k') > 0
			replace data_`k' = "" if real(substr(data_`k', 1, 4)) < 1900
		}
		
		gen aux_data_nascimento = date(data_nascimento, "YMD")
		format aux_data_nascimento %td
		gen idade = round((td(01oct`ano') - aux_data_nascimento) / 365.25, 1)
		replace idade = . if idade < 15 | idade > 100
		order idade, a(data_nascimento)
		drop aux_data_nascimento
		
		cap drop aux*
		
		//------------------//
		// ajustes e salva
		//------------------//
		
		drop if ano == .
		
		duplicates drop
		
		tempfile candidatos_`uf'_`ano'
		save `candidatos_`uf'_`ano''
		
	}
	*
	
	//------------------//
	// append
	//------------------//
	
	use `candidatos_AC_`ano'', clear
	foreach uf in `ufs_`ano'' {
		if "`uf'" != "AC" {
			append using `candidatos_`uf'_`ano''
		}
	}
	*
	
	compress
	
	save "output/candidatos_`ano'.dta", replace
	
}
*
