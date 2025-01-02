
//----------------------------------------------------------------------------//
// prefacio
//----------------------------------------------------------------------------//

clear all
cap log close
set more off
set varabbrev off

cd "~/Downloads/dados_TSE"

do "~/Dropbox/BD/sdk/bases/br_tse_eleicoes/code/fnc/clean_string.do"
do "~/Dropbox/BD/sdk/bases/br_tse_eleicoes/code/fnc/limpa_tipo_eleicao.do"
do "~/Dropbox/BD/sdk/bases/br_tse_eleicoes/code/fnc/limpa_instrucao.do"
do "~/Dropbox/BD/sdk/bases/br_tse_eleicoes/code/fnc/limpa_estado_civil.do"
do "~/Dropbox/BD/sdk/bases/br_tse_eleicoes/code/fnc/limpa_resultado.do"
do "~/Dropbox/BD/sdk/bases/br_tse_eleicoes/code/fnc/limpa_partido.do"
do "~/Dropbox/BD/sdk/bases/br_tse_eleicoes/code/fnc/limpa_candidato.do"

//----------------------------------------------------------------------------//
// build
//----------------------------------------------------------------------------//

do "code/sub/candidatos.do"
do "code/sub/partidos.do"
do "code/sub/detalhes_votacao_municipio_zona.do"
do "code/sub/detalhes_votacao_secao.do"
do "code/sub/perfil_eleitorado_municipio_zona.do"
do "code/sub/perfil_eleitorado_secao.do"
do "code/sub/perfil_eleitorado_local_votacao.do"
do "code/sub/vagas.do"
do "code/sub/resultados_municipio_zona.do"
do "code/sub/resultados_secao.do"
do "code/sub/prestacao_contas.do"

do "code/sub/normalizacao_particao.do"
do "code/sub/agregacao.do"

do "code/sub/local_votacao.do"
