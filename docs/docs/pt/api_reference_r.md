# R

Esta API é composta somente de módulos para **requisição de dados**, ou
seja, download e/ou carregamento de dados do projeto no seu ambiente de
análise).
Para fazer **gerenciamento de dados** no Google Cloud, busque as funções
na API de [linha de comando](../api_reference_cli) ou em [Python](../api_reference_python/#classes-gerenciamento-de-dados).

A documentação completa encontra-se na página do CRAN do projeto, e
segue baixo.

!!! Info "Toda documentação do código abaixo está em inglês"

<object data="https://cran.r-project.org/web/packages/basedosdados/basedosdados.pdf" type="application/pdf" width="700px" height="700px">
    <embed src="https://cran.r-project.org/web/packages/basedosdados/basedosdados.pdf">
        <p>This browser does not support PDFs. Please download the PDF to view it: <a href="https://cran.r-project.org/web/packages/basedosdados/basedosdados.pdf">Download PDF</a>.</p>
    </embed>
</object>

## Ih rapaz, deu erro! E agora?
Os principais erros encontrados do pacote da Base dos Dados no Rstudio são derivados de dois fatores:

    * Autenticação

    * Versão do pacote `dbplyr`

Portanto, se algum erro aparecer para você, por favor, tente primeiro checar se ele está relacionado a esses dois fatores.

### Autenticação
A maioria dos erros do nosso pacote estão relacionados a problemas de autenticação. O pacote `basedosdados` requer que o usuário forneça todas as autenticações solicitadas pela função `basedosdados::set_billing_id`, inclusive aquelas que aparecem como optativas. Por isso, é necessário estar atento se você marcou todas as caixinhas de seleção quando o Rstudio disponibiliza essa tela no navegador:

![Capturar](https://user-images.githubusercontent.com/26544494/190700064-1326a74c-8de0-4254-a562-32f9aa10ae07.PNG)

**Note que é preciso marcar inclusive as duas últimas "caixinhas", que aparecem como opcionais**. Caso você tenha esquecido de marcá-las, todas as outras funções do pacote não irão funcionar posteriormente.

Caso você já tenha autenticado com autorização incompleta, é preciso repetir o processo de autenticação. Você pode fazer isso rodando `gargle::gargle_oauth_sitrep()`. Você deverá checar a pasta em que estão salvas as autenticações do seu R, entrar nesta pasta e deletar aquela referente ao Google Cloud/Bigquery. Feito isso, ao rodar `basedosdados::set_billing_id` você poderá autenticar novamente.

Veja como é simples:

![gif_gargle](https://user-images.githubusercontent.com/62671380/194094167-99dadbd7-f7de-46f9-ac88-fb464e646e6c.gif)

Realizados todos esses procedimentos, é bem provável que os erros anteriores não ocorram mais.

### Versão do pacote `dbplyr`
Outro erro comum está relacionado ao uso da função `basedosdados::bdplyr`. Nosso pacote em R foi construído utilizando outros pacotes disponíveis na comunidade. Isso significa que atualizações destes pacotes podem alterar o funcionamento destes e gerar efeitos em cascata a outros pacotes desenvolvidos em cima deles. Neste contexto, o nosso pacote funciona apenas com a versão 2.1.1 do pacote `dbplyr`, e **não** funciona com versões posteriores.

Você pode checar a sua versão do `dbplyr` rodando `utils::packageVersion("dbplyr")` no seu R. Caso ela seja superior à versão 2.1.1, você precisa dar um _downgrade_ para a versão correta. Para isso, você pode rodar `devtools::install_version("dbplyr", version = "2.1.1", repos = "http://cran.us.r-project.org")`.

### Outros erros
Caso os erros persistam, você pode abrir uma _issue_ no nosso Github clicando [aqui](https://github.com/basedosdados/mais/issues). Você também visitar as _issues_ que já foram resolvidas e estão atribuídas com o a etiqueta `R` em nosso Github [aqui](https://github.com/basedosdados/mais/issues?q=is%3Aissue+is%3Aclosed).
