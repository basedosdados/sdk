
//----------------------------------------------------------------------------//
// build: resultados municipio-zona
//----------------------------------------------------------------------------//

//------------------------//
// listas de estados
//------------------------//

local ufs_1994_candidato	AC AL AM AP BA BR              GO MA    MS       PB PE PI          RO RR RS SC SE SP TO
local ufs_1996_candidato	AC AL AM AP BA        CE    ES GO MA MG MS    PA PB PE PI       RN RO RR RS    SE SP TO
local ufs_1998_candidato	AC AL AM AP BA BRASIL CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2000_candidato	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2002_candidato	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2004_candidato	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2006_candidato	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2008_candidato	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2010_candidato	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2012_candidato	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2014_candidato	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2016_candidato	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2018_candidato	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2020_candidato	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO	
local ufs_2022_candidato	AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2024_candidato	AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO	

local ufs_1994_partido		AC AL AM AP BA BR              GO MA    MS       PB PE PI          RO RR RS SC SE SP TO
local ufs_1996_partido		AC AL AM AP BA        CE    ES GO MA MG MS    PA PB PE PI       RN RO RR RS    SE SP TO
local ufs_1998_partido		AC AL AM AP BA BRASIL CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2000_partido		AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2002_partido		AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2004_partido		AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2006_partido		AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2008_partido		AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2010_partido		AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2012_partido		AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2014_partido		AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2016_partido		AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2018_partido		AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO	
local ufs_2020_partido		AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2022_partido		AC AL AM AP BA BR     CE DF ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO
local ufs_2024_partido		AC AL AM AP BA        CE    ES GO MA MG MS MT PA PB PE PI PR RJ RN RO RR RS SC SE SP TO

//------------------------//
// loops
//------------------------//

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) case(preserve)
keep id_municipio id_municipio_tse
tempfile diretorio
save `diretorio'

import delimited "input/br_bd_diretorios_brasil_municipio.csv", clear varn(1) case(preserve)
keep id_municipio_tse sigla_uf
tempfile diretorio_ufs
save `diretorio_ufs'

foreach ano of numlist 1994(2)2024 {
	
	foreach tipo in candidato partido { // 
		
		if "`tipo'" == "candidato" {
			
			foreach uf in `ufs_`ano'_candidato' {
				
				cap import delimited "input/votacao_candidato_munzona/votacao_candidato_munzona_`ano'/votacao_candidato_munzona_`ano'_`uf'.txt", ///
					delim(";") varn(nonames) stringcols(_all) clear
				cap import delimited "input/votacao_candidato_munzona/votacao_candidato_munzona_`ano'/votacao_candidato_munzona_`ano'_`uf'.csv", ///
					delim(";") varn(nonames) stringcols(_all) clear
				
				if `ano' == 1994 {
					
					drop in 1
					
					keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v19 v20 v21 v22 v29 v30 v40 v44
					
					ren v3  ano
					ren v6  turno
					ren v7  id_eleicao
					ren v8  tipo_eleicao
					ren v9  data_eleicao
					ren v11 sigla_uf
					ren v14 id_municipio_tse
					ren v16 zona
					ren v18 cargo
					ren v19 sequencial_candidato
					ren v20 numero_candidato
					ren v21 nome_candidato
					ren v22 nome_urna_candidato
					ren v29 numero_partido
					ren v30 sigla_partido
					ren v40 votos
					ren v44 resultado
					
				}
				else if `ano' == 1996 | (`ano' >= 2002 & `ano' <= 2016) {
					
					drop in 1
					
					keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v19 v20 v21 v22 v29 v30 v36 v38
					
					ren v3  ano
					ren v6  turno
					ren v7  id_eleicao
					ren v8  tipo_eleicao
					ren v9  data_eleicao
					ren v11 sigla_uf
					ren v14 id_municipio_tse
					ren v16 zona
					ren v18 cargo
					ren v19 sequencial_candidato
					ren v20 numero_candidato
					ren v21 nome_candidato
					ren v22 nome_urna_candidato
					ren v29 numero_partido
					ren v30 sigla_partido
					ren v36 resultado
					ren v38 votos
					
				}
				else if (`ano' >= 1998 & `ano' <= 2000) | (`ano' >= 2018 & `ano' <= 2022) {
					
					drop in 1
					
					keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v19 v20 v21 v22 v29 v30 v42 v44
					
					ren v3  ano
					ren v6  turno
					ren v7  id_eleicao
					ren v8  tipo_eleicao
					ren v9  data_eleicao
					ren v11 sigla_uf
					ren v14 id_municipio_tse
					ren v16 zona
					ren v18 cargo
					ren v19 sequencial_candidato
					ren v20 numero_candidato
					ren v21 nome_candidato
					ren v22 nome_urna_candidato
					ren v29 numero_partido
					ren v30 sigla_partido
					ren v42 votos
					ren v44 resultado
					
				}
				else if (`ano' >= 2024) {
					
					drop in 1
					
					keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v19 v20 v21 v22 v35 v36 v46 v50
					
					ren v3  ano
					ren v6  turno
					ren v7  id_eleicao
					ren v8  tipo_eleicao
					ren v9  data_eleicao
					ren v11 sigla_uf
					ren v14 id_municipio_tse
					ren v16 zona
					ren v18 cargo
					ren v19 sequencial_candidato
					ren v20 numero_candidato
					ren v21 nome_candidato
					ren v22 nome_urna_candidato
					ren v35 numero_partido
					ren v36 sigla_partido
					ren v46 votos
					ren v50 resultado
					
				}
				
				//------------------//
				// limpa strings
				//------------------//
				
				destring ano turno id_municipio_tse votos, replace force // remover zeros a esquerda
				
				replace sequencial_candidato = "" if sequencial_candidato == "-1"
				
				replace resultado = "2º turno" if resultado == "2O. TURNO"
				
				/*
				if "`uf'" == "BR" | "`uf'" == "BRASIL" {
					ren sigla_uf sigla_uf_orig
					merge m:1 id_municipio_tse using `diretorio_ufs'
					drop if _merge == 2
					drop _merge
					replace sigla_uf_orig = sigla_uf if sigla_uf_orig == "BR"
					replace sigla_uf_orig = "ZZ"     if sigla_uf_orig == ""
					drop sigla_uf
					ren sigla_uf_orig sigla_uf
				}
				*/
				
				foreach k of varlist _all {
					cap replace `k' = ""  if inlist(`k', "#NULO#", "#NULO", "#NE#", "#NE", "##VERIFICAR BASE 1994##")
				}
				*
				
				foreach k in tipo_eleicao cargo resultado {
					clean_string `k'
				}
				foreach k in nome_candidato nome_urna_candidato {
					replace `k' = ustrtitle(`k')
				}
				*
				
				foreach k in eleicao {
					replace data_`k' = substr(data_`k', 7, 4) + "-" + substr(data_`k', 4, 2) + "-" + substr(data_`k', 1, 2)
					replace data_`k' = "" if real(substr(data_`k', 1, 4)) < 1900
				}
				
				limpa_tipo_eleicao	`ano'
				limpa_partido		`ano' sigla_partido
				limpa_resultado
				
				cap limpa_candidato
				
				drop if turno == 2 & cargo == "presidente" & "`uf'" != "BR" & `ano' == 2002
					// consertando problema que dados para presidente no 2o turno vem dobrados entre arquivos de UFs e BR em 2002.
				drop if cargo == "presidente" & "`uf'" != "BR" & (`ano' == 2006 | `ano' == 2010)
				
				if `ano' == 1998 {
					if "`uf'" == "BRASIL" {
						keep if cargo == "presidente"
					}
					else {
						drop if cargo == "presidente"
					}
				}
				
				
				tempfile resultados_cand_`uf'_`ano'
				save `resultados_cand_`uf'_`ano''
				
			}
			
			//------------//
			// append
			//------------//
			
			use `resultados_cand_AC_`ano'', clear
			foreach uf in `ufs_`ano'_candidato' {
				if "`uf'" != "AC" {
					append using `resultados_cand_`uf'_`ano''
				}
			}
			*
			
			collapse (sum) votos, by(ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio_tse zona cargo *_candidato *_partido resultado) // soma potenciais votos em transito e nao
			
			merge m:1 id_municipio_tse using `diretorio', keep(1 3) nogenerate
			
			order id_municipio, b(id_municipio_tse)
			
			/*
			//-------------------------//
			// conserta problema de
			// resultado nulo para UFs=VT,ZZ
			//-------------------------//
			
			preserve
				
				keep if cargo == "presidente"
				
				bys numero_candidato turno (resultado): replace resultado = resultado[_N] if cargo == "presidente"	
				
				tempfile presid
				save `presid'
				
			restore
			
			drop if cargo == "presidente"
			append using `presid'
			*/
			
			//-------------------------//
			// salva
			//-------------------------//
			
			compress
			
			save "output/resultados_candidato_municipio_zona_`ano'.dta", replace
			
		}
		*
		
		if "`tipo'" == "partido" {
			
			foreach uf in `ufs_`ano'_partido' {
				
				di "`ano'_`uf'_partido"
				
				cap import delimited "input/votacao_partido_munzona/votacao_partido_munzona_`ano'/votacao_partido_munzona_`ano'_`uf'.txt", ///
					delim(";") varn(nonames) stringcols(_all) clear
				cap import delimited "input/votacao_partido_munzona/votacao_partido_munzona_`ano'/votacao_partido_munzona_`ano'_`uf'.csv", ///
					delim(";") varn(nonames) stringcols(_all) clear
				
				if `ano' == 1994 | `ano' == 1998 {
					
					drop in 1
					
					keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v20 v21 v33 v34
					
					ren v3	ano
					ren v6	turno
					ren v7  id_eleicao
					ren v8	tipo_eleicao
					ren v9  data_eleicao
					ren v11	sigla_uf
					ren v14	id_municipio_tse
					ren v16	zona
					ren v18	cargo
					ren v20 numero_partido
					ren v21	sigla_partido
					ren v33	votos_legenda
					ren v34	votos_nominais
					
					order votos_nominais, b(votos_legenda)
					
				}
				else if `ano' == 1996 | (`ano' >= 2000 & `ano' <= 2016) {
					
					drop in 1
					
					keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v20 v21 v27 v28
					
					ren v3	ano
					ren v6	turno
					ren v7  id_eleicao
					ren v8	tipo_eleicao
					ren v9  data_eleicao
					ren v11	sigla_uf
					ren v14	id_municipio_tse
					ren v16	zona
					ren v18	cargo
					ren v20 numero_partido
					ren v21	sigla_partido
					ren v27	votos_nominais
					ren v28	votos_legenda
					
				}
				else if (`ano' >= 2018 & `ano' <= 2020) {
					
					drop in 1
					
					keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v20 v21 v33 v34
					
					ren v3	ano
					ren v6	turno
					ren v7  id_eleicao
					ren v8	tipo_eleicao
					ren v9  data_eleicao
					ren v11	sigla_uf
					ren v14	id_municipio_tse
					ren v16	zona
					ren v18	cargo
					ren v20 numero_partido
					ren v21	sigla_partido
					ren v33	votos_legenda
					ren v34	votos_nominais
					
					order votos_nominais, b(votos_legenda)
					
				}
				else if `ano' >= 2022 {
					
					drop in 1
					
					keep v3 v6 v7 v8 v9 v11 v14 v16 v18 v20 v21 v33 v34
					
					ren v3	ano
					ren v6	turno
					ren v7  id_eleicao
					ren v8	tipo_eleicao
					ren v9  data_eleicao
					ren v11	sigla_uf
					ren v14	id_municipio_tse
					ren v16	zona
					ren v18	cargo
					ren v20 numero_partido
					ren v21	sigla_partido
					ren v33	votos_legenda
					ren v34	votos_nominais
					
					order votos_nominais, b(votos_legenda)
					
				}
				*
				
				//------------------//
				// limpa strings
				//------------------//
				
				destring ano turno id_municipio_tse votos_*, replace force // remover zeros a esquerda
				
				/*
				if "`uf'" == "BR" | "`uf'" == "BRASIL" {
					ren sigla_uf sigla_uf_orig
					merge m:1 id_municipio_tse using `diretorio_ufs'
					drop if _merge == 2
					drop _merge
					replace sigla_uf_orig = sigla_uf if sigla_uf_orig == "BR"
					replace sigla_uf_orig = "ZZ"     if sigla_uf_orig == ""
					drop sigla_uf
					ren sigla_uf_orig sigla_uf
				}
				*/
				
				foreach k of varlist _all {
					cap replace `k' = ""  if inlist(`k', "#NULO#", "#NULO", "#NE#", "#NE", "##VERIFICAR BASE 1994##")
				}
				*
				
				foreach k in tipo_eleicao cargo {
					clean_string `k'
				}
				
				limpa_tipo_eleicao	`ano'
				limpa_partido		`ano' sigla_partido
				
				foreach k in eleicao {
					replace data_`k' = substr(data_`k', 7, 4) + "-" + substr(data_`k', 4, 2) + "-" + substr(data_`k', 1, 2) if data_`k' != ""
					replace data_`k' = "" if real(substr(data_`k', 1, 4)) < 1900
				}
				
				collapse (sum) votos*, by(ano turno id_eleicao tipo_eleicao data_eleicao sigla_uf id_municipio_tse zona cargo numero_partido sigla_partido) // soma votos em transito e nao
				
				merge m:1 id_municipio_tse using `diretorio', keep(1 3) nogenerate
				
				order id_municipio, b(id_municipio_tse)
				
				// para 2006 e cargo=presidente, dados originais tem problemas: sigla_uf=BR duplica números, sigla_uf=DF tem votos x4
				// e outros erros que não consegui encontrar
				// solução: usar *só* o arquivo sigla_uf=BR, que vem correto
				if `ano' == 2006 & "`uf'" != "BR" {
					drop if cargo == "presidente"
				}
				
				if `ano' == 1998 & "`uf'" != "BRASIL" {
					keep if cargo == "presidente"
				}
				
				cap duplicates drop
				
				tempfile resultados_part_`uf'_`ano'
				save `resultados_part_`uf'_`ano''
				
			}
			
			//------------//
			// append
			//------------//
			
			use `resultados_part_AC_`ano'', clear
			foreach uf in `ufs_`ano'_partido' {
				if "`uf'" != "AC" {
					append using `resultados_part_`uf'_`ano''
				}
			}
			*
			
			compress
			
			save "output/resultados_partido_municipio_zona_`ano'.dta", replace
			
		}
	}
}
*
