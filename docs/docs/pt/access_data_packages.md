# Pacotes

Os pacotes da Base dos Dados permitem o acesso ao *datalake* público
direto do seu computador ou ambiente de desenvolvimento. Atualmente disponíveis em:

- **:material-language-python: Python**
- **:material-language-r: R**
- **Stata**
- **:octicons-terminal-16: CLI (terminal)**

Pronto(a) para começar? Nesta página você encontra:

- [Primeiros passos](#primeiros-passos)
- [Tutoriais](#tutoriais)
- [Manuais de referência](#manuais-de-referencia-api)

## Primeiros passos

### Antes de começar: Crie o seu projeto no Google Cloud

Para criar um projeto no Google Cloud basta ter um email cadastrado no
Google. É necessário ter um projeto seu, mesmo que vazio, para você
fazer queries em nosso *datalake* público.

1. **[Acesse o Google Cloud](https://console.cloud.google.com/projectselector2/home/dashboard)**.
   Caso for a sua primeira vez, aceite o Termo de Serviços.
2. **Clique em `Create Project/Criar Projeto`**. Escolha um nome bacana para o projeto.
3. **Clique em `Create/Criar`**

??? Info "Por que eu preciso criar um projeto no Google Cloud?"
    A Google fornece 1 TB gratuito por mês de uso do BigQuery para cada
    projeto que você possui. Um projeto é necessário para ativar os
    serviços do Google Cloud, incluindo a permissão de uso do BigQuery.
    Pense no projeto como a "conta" na qual a Google vai contabilizar o
    quanto de processamento você já utilizou. **Não é necessário adicionar
    nenhum cartão ou forma de pagamento - O BigQuery inicia automaticamente no modo Sandbox, que permite você utilizar seus recursos sem adicionar um modo de pagamento. [Leia mais aqui](https://cloud.google.com/bigquery/docs/sandbox/?hl=pt).**

### Instalando o pacote

Para instalação do pacote em Python e linha de comando, você pode usar o
`pip` direto do seu terminal. Em R, basta instalar diretamente no
RStudio ou editor de sua preferência.

=== "**Python/CLI**"
    ```bash
    pip install basedosdados
    ```

=== "**R**"
    ```R
    install.packages("basedosdados")
    ```

=== "**Stata**"

Requerimentos:

1. Garantir que seu Stata seja a __versão 16+__
2. Garantir que o Python esteja instalado no seu computador.

Com os requerimentos satisfeitos, rodar os comandos abaixo:
```stata
net install basedosdados, from("https://raw.githubusercontent.com/basedosdados/mais/master/stata-package")
```

### Configurando o pacote

Uma vez com seu projeto, você precisa configurar o pacote para usar o ID
desse projeto nas consultas ao *datalake*. Para isso, você deve usar o
`project_id` que a Google fornece para você assim que o
projeto é criado.

![Exemplo de ID do Projeto no BigQuery](images/project_id_example.png)

=== "Python/CLI"
    *Não é necessário configurar o projeto de antemão. Assim que você
    roda a 1ª consulta, o pacote irá indicar os passos para configuração.*

=== "R"
    *Uma vez com o `project_id`, você deve passar essa
    informação para o pacote usando a função `set_billing_id`.*
    ```R
    set_billing_id("<YOUR_PROJECT_ID>")
    ```

=== "Stata"
    *É necessário especificar o `project_id` a cada vez que usar o pacote.*


### Faça sua primeira consulta

Um exemplo simples para começar a explorar o *datalake* é puxar informações cadastrais de
municípios direto na nossa base de [Diretórios Brasileiros (tabela `municipio`)](https://basedosdados.org/dataset/br-bd-diretorios-brasil). Para isso, vamos usar a
função `download`, baixando os dados direto para nossa máquina.

=== "Python"
    ```python
    import basedosdados as bd
    bd.download(savepath="<PATH>",
    dataset_id="br-bd-diretorios-brasil", table_id="municipio")
    ```

    *Para entender mais sobre a função `download`, leia o [manual de referência](../api_reference_python).*

=== "R"
    ```R
    library("basedosdados")
    query <- "SELECT * FROM `basedosdados.br_bd_diretorios_brasil.municipio`"
    dir <- tempdir()
    data <- download(query, "<PATH>")
    ```

    *Para entender mais sobre a função `download`, leia o [manual de referência](../api_reference_r).*

=== "Stata"
    ```stata
    bd_read_sql, ///
        path("<PATH>") ///
        query("SELECT * FROM `basedosdados.br_bd_diretorios_brasil.municipio`") ///
        billing_project_id("<PROJECT_ID>")
    ```

=== "CLI"
    ```bash
    basedosdados download "where/to/save/file" \
    --billing_project_id <YOUR_PROJECT_ID> \
    --query 'SELECT * FROM
    `basedosdados.br_bd_diretorios_brasil.municipio`'
    ```
    *Para entender mais sobre a função `download`, leia o [manual de referência](../api_reference_cli).*

## Tutoriais

### Como usar os pacotes

Preparamos tutoriais apresentando as principais funções de cada pacote
para você começar a usá-los.

=== "**Python**"
    Blog:

    - [Introdução ao pacote Python](https://dev.to/basedosdados/base-dos-dados-python-101-44lc)
    - [Introdução ao pacote Python (cont.)](https://dev.to/basedosdados/base-dos-dados-python-102-50k0)

    Vídeos:

    - [Workshop: Aplicações em Python](https://www.youtube.com/watch?v=wI2xEioDPgM)

=== "**R**"
    Blog:

    - [Introdução ao pacote R](https://dev.to/basedosdados/como-usar-a-biblioteca-basedosdados-no-r-capitulo-1-46kb)
    - [Explorando o Censo Escolar](https://dev.to/basedosdados/explorando-o-censo-escolar-com-a-base-dos-dados-1a89)
    - [Análise: O Brasil nas Olimpíadas](https://dev.to/basedosdados/o-brasil-nas-olimpiadas-2g6n)

    Vídeos:

    - [Workshop: Aprenda a acessar dados públicos em R](https://www.youtube.com/watch?v=M9ayiseIjvI&t=250s)

=== "**Stata**"
    Documentação:

    - [GitHub](https://github.com/basedosdados/mais/tree/master/stata-package)

## Manuais de referência (API)

* [:material-language-python: Python](../api_reference_python)
* [:material-language-r: R](../api_reference_r)
* [Stata](../api_reference_stata)
* [:octicons-terminal-16: CLI](../api_reference_cli)
