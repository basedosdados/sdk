# Suba dados na BD

## Por que minha organização deve subir dados na BD?

- **Capacidade de cruzar suas bases com dados de diferentes
  organizações** de forma simples e fácil. Já são centenas de conjuntos
  de dados públicos das maiores organizações do Brasil e do mundo presentes
  no nosso *datalake*.

- **Compromisso com a transparência, qualidade dos dados e
  desenvolvimento de melhores pesquisas, análises e soluções** para a
  sociedade. Não só democratizamos o acesso a dados abertos, mas também dados
  de qualidade. Temos um time especializado que revisa e garante a qualidade dos
  dados adicionados ao *datalake*.

- **Participação de uma comunidade que cresce cada vez mais**: milhares
  de jornalistas, pesquisadores(as), desenvolvedores(as), já utilizam e
  acompanham a Base dos Dados.
  <!-- TODO: Colocar aqui o link do nosso painel de audiencia quando tiver pronto :) -->

## Passo a passo para subir dados

Quer subir dados na BD e nos ajudar a construir esse repositório?
*Maravilha!* Organizamos tudo o que você precisa no manual abaixo em 8 passos

Para facilitar a explicação, vamos seguir um exemplo já pronto com dados da [RAIS](https://basedosdados.org/dataset/br-me-rais).

!!! Tip "Você pode navegar pelas etapas no menu à esquerda."
    Sugerimos fortemente que entre em nosso [canal no
    Discord](https://discord.gg/huKWpsVYx4) para tirar dúvidas e
    interagir com a equipe e outros(as) colaboradores(as)! 😉

### Antes de começar

Alguns conhecimentos são necessárias para realizar esse processo:

- **Python, R, SQL e/ou Stata**: para criar os códigos de captura e limpeza dos dados.
- **Linha de comando**: para configurar seu ambiente local
  e conexão com o Google Cloud.
- **Github**: para subir seu código para revisão da
  nossa equipe.

!!! Tip "Não tem alguma dessas habilidades, mas quer colaborar?"
    Temos um time de dados que pode te ajudar, basta entrar no [nosso
    Discord](https://discord.gg/huKWpsVYx4) e mandar uma mensagem em #quero-contribuir.

### Como funciona o processo?

- [1. Escolher a base e entender mais dos dados](#1-escolher-a-base-e-entender-mais-dos-dados) - primeiro precisamos conhecer o que estamos tratando.
- [2. Baixar nossa pasta template](#2-baixar-nossa-pasta-template) - é hora estruturar o trabalho a ser feito
- [3. Preencher as tabelas de arquitetura](#3-preencher-as-tabelas-de-arquitetura) - é primordial definir a estrutura dos dados antes de iniciarmos o tratamento
- [4. Escrever codigo de captura e limpeza de dados](#4-escrever-codigo-de-captura-e-limpeza-de-dados) - hora de botar a mão na massa!
- [5. (Caso necessário) Organizar arquivos auxiliares](#5-caso-necessario-organizar-arquivos-auxiliares) -  porque até dados precisam de guias
- [6. (Caso necessário) Criar tabela dicionário](#6-caso-necessario-criar-tabela-dicionario) - momento de montar os dicionários
- [7. Subir tudo no Google Cloud](#7-subir-tudo-no-google-cloud) - afinal, é por lá que ficam os dados da BD
- [8. Enviar tudo para revisão](#8-enviar-tudo-para-revisao) - um olhar da nossa equipe para garantir que tudo está pronto para ir pra produção!


### 1. Escolher a base e entender mais dos dados

Mantemos a lista de conjuntos para voluntários no nosso [Github](https://github.com/orgs/basedosdados/projects/17/views/10). Para começar a subir uma base do seu interesse, basta abrir uma [nova issue](https://github.com/basedosdados/queries-basedosdados/issues/new?assignees=&labels=&projects=&template=issue--novos-dados.md&title=%5Bdados%5D+%3Cdataset_id%3E) de dados. Caso sua base (conjunto) já esteja listada, basta marcar seu usuário do Github como `assignee`

Seu primeiro trabalho é preencher as informações na issue. Essas informações vão te ajudar a entender melhor os dados e serão muito úteis para o tratamento e o preenchimento de metadados.

Quando finalizar essa etapa, chame alguém da equipe dados para que as informações que você mapeou sobre o conjunto já entrem pro nosso site!
### 2. Baixar nossa pasta template

[Baixe aqui a pasta
_template_](https://drive.google.com/drive/folders/1xXXon0vdjSKr8RCNcymRdOKgq64iqfS5?usp=sharing)
 e renomeie para o `<dataset_id>` (definido na issue do [passo 1](#-1-Escolher-a-base-e-entender-mais-dos-dados)). Essa pasta template facilita e organiza todos os
passos daqui pra frente. Sua
estrutura é a seguinte:

- `<dataset_id>/`
    - `code/`: Códigos necessários para **captura** e **limpeza** dos dados
    ([vamos ver mais no passo
    4](#4-escrever-codigo-de-captura-e-limpeza-de-dados)).
    - `input/`: Contém todos os arquivos com dados originais, exatamente
    como baixados da fonte primária. ([vamos ver mais no passo
    4](#4-escrever-codigo-de-captura-e-limpeza-de-dados)).
    - `output/`: Arquivos finais, já no formato pronto para subir na BD ([vamos ver mais no passo
    4](#4-escrever-codigo-de-captura-e-limpeza-de-dados)).
    - `tmp/`: Quaisquer arquivos temporários criados pelo código em `/code` no processo de limpeza e tratamento ([vamos ver mais no passo
    4](#4-escrever-codigo-de-captura-e-limpeza-de-dados)).
    - `extra/`
        - `architecture/`: Tabelas de arquitetura ([vamos ver mais no passo 3](#3-preencher-as-tabelas-de-arquitetura)).
        - `auxiliary_files/`: Arquivos auxiliares aos dados ([vamos ver mais no passo 5](#5-caso-necessário-organizar-arquivos-auxiliares)).
        - `dicionario.csv`: Tabela dicionário de todo o conjunto de dados ([vamos ver mais no passo 6](#6-caso-necessário-criar-tabela-dicionário)).

!!! info "Apenas a pasta `code` será commitada para o seu projeto, os demais arquivos existirão apenas localmente ou no Google Cloud."

### 3. Preencher as tabelas de arquitetura

As tabelas de arquitetura determinam **qual a estrutura de
cada tabela do seu conjunto de dados**. Elas definem, por exemplo, o nome, ordem e metadados das variáveis, além de compatibilizações quando há mudanças em versões (por
exemplo, se uma variável muda de nome de um ano para o outro).

!!! Info "Cada tabela do conjunto de dados deve ter sua própria tabela de arquitetura (planilha), que deve ser preenchida no **Google Drive** para permitir a correção pela nossa equipe de dados."


#### Exemplo: RAIS - Tabelas de arquitetura

As tabelas de arquitetura da RAIS [podem ser consultadas aqui](https://docs.google.com/spreadsheets/d/1dPLUCeE4MSjs0ykYUDsFd-e7-9Nk6LVV/edit?usp=sharing&ouid=103008455637924805982&rtpof=true&sd=true). Elas são uma ótima referência para você começar seu trabalho já que tem muitas variáveis e exemplos de diversas situações que você pode acabar encontrando.

#### Para o preenchimento de cada tabela do seu conjunto siga esse passo a passo:

!!! Tip "A cada início e final de etapa consulte nosso [manual de estilo](style_data.md) para garantir que você está seguindo a padronização da BD"

1. Listar todas as variáveis dos dados na coluna `original_name`
    - Obs: Caso a base mude o nome das variáveis ao longo dos anos (como a RAIS), é necessário fazer a compatibilização entre anos para todas as variáveis preenchendo a coluna de `original_name_YYYY` para cada ano ou mês disponível
2. Renomear as variáveis conforme nosso [manual](style_data.md) na coluna `name`
3. Entender o tipo da variável e preencher a coluna `bigquery_type`
4. Preencher a descrição em `description` conforme o [manual](style_data.md)
5. A partir da compatibilização entre anos e/ou consultas aos dados brutos, preencher a cobertura temporal em `temporal_coverage` de cada variável
    - Obs: Caso as variáveis tenham a mesma cobertura temporal da tabela preencher apenas com '(1)'
6. Indicar com 'yes' ou 'no' se há dicionário para as variáveis em `covered_by_dictionary`
7. Verificar se as variáveis representam alguma entidade presente nos [diretórios](https://basedosdados.org/dataset?q=diret%C3%B3rio&datasets_with=open_tables&organization=bd&page=1) para preencher o `directory_column`
8. Para as variáveis do tipo `int64` ou `float64` verificar se é necessário incluir uma [unidade de medida](https://github.com/basedosdados/website/blob/master/ckanext-basedosdados/ckanext/basedosdados/validator/available_options/measurement_unit.py)
9. Reordernar as variáveis conforme o [manual](style_data.md)

!!! Tip "Quando terminar de preencher as tabelas de arquitetura, entre em contato com a equipe da Base dos Dados para validar tudo. É necessário que esteja claro o formato final que os dados devem ficar _antes_ de começar a escrever o código. Assim a gente evita o retrabalho."

### 4. Escrever codigo de captura e limpeza de dados

Após validadas as tabelas de arquitetura, podemos escrever os códigos de
**captura** e **limpeza** dos dados.

- **Captura**: Código que baixa automaticamente todos os dados originais e os salva em `/input`. Esses dados podem estar disponíveis em portais ou links FTP, podem ser raspados de sites, entre outros.

- **Limpeza**: Código que transforma os dados originais salvos em `/input` em dados limpos, salva na pasta `/output`, para, posteriormente, serem subidos na BD.

Cada tabela limpa para produção pode ser salva como um arquivo único ou, caso seja muito grande (e.g. acima de 200 mb), ser particionada no formato [Hive](https://cloud.google.com/bigquery/docs/hive-partitioned-loads-gcs) em vários sub-arquivos. Os formatos aceitos são `.csv` ou `.parquet`. Nossa recomendação é particionar tabelas por `ano`, `mes` e `sigla_uf`. O particionamento é feito através da estrutura de pastas, veja o exemplo a baixo para visualizar como.

#### Exemplo: RAIS - Particionamento
A tabela `microdados_vinculos` da RAIS, por exemplo, é uma tabela muito grande (+250GB) por isso nós particionamos por `ano` e `sigla_uf`. O particionamento foi feito usando a estrutura de pastas `/microdados_vinculos/ano=YYYY/sigla_uf=XX` .

#### Padrões necessários no código

- Devem ser escritos em [Python](https://www.python.org/),
  [R](https://www.r-project.org/) ou [Stata](https://www.stata.com/) -
  para que a revisão possa ser realizada pela equipe.
- Pode estar em script (`.py`, `.R`, ...) ou *notebooks* (Google Colab, Jupyter, Rmarkdown, etc).
- Os caminhos de arquivos devem ser atalhos _relativos_ à pasta raíz
  (`<dataset_id>`), ou seja, não devem depender dos caminhos do seu
  computador.
- A limpeza deve seguir nosso [manual de estilo](style_data.md) e as [melhores práticas de programação](https://en.wikipedia.org/wiki/Best_coding_practices).

#### Exemplo: PNAD Contínua - Código de limpeza

O código de limpeza foi construído em R e [pode ser consultado
aqui](https://github.com/basedosdados/mais/tree/master/bases/br_ibge_pnadc/code).

#### Exemplo: Atividade na Câmara Legislativa - Código de download e limpeza
O código de limpeza foi construído em Python [pode ser consultado aqui](https://github.com/basedosdados/mais/tree/bea9a323afcea8aa1609e9ade2502ca91f88054c/bases/br_camara_atividade_legislativa/code)



### 5. (Caso necessário) Organizar arquivos auxiliares

É comum bases de dados serem disponibilizadas com arquivos auxiliares. Esses podem incluir notas técnicas, descrições de coleta e amostragem, etc. Para ajudar usuários da Base dos Dados terem mais contexto e entenderem melhor os dados, organize todos esses arquivos auxiliares em `/extra/auxiliary_files`.

Fique à vontade para estruturar sub-pastas como quiser lá dentro. O que importa é que fique claro o que são esses arquivos.

### 6. (Caso necessário) Criar tabela dicionário

Muitas vezes, especialmente com bases antigas, há múltiplos dicionários em formatos Excel ou outros. Na Base dos Dados nós unificamos tudo em um único arquivo em formato `.csv` - um único dicionário para todas as colunas de todas as tabelas do seu conjunto.

!!! Info "Detalhes importantes de como construir seu dicionário estão no nosso [manual de estilo](../style_data/#dicionarios)."

#### Exemplo: RAIS - Dicionário

O dicionário completo [pode ser consultado
aqui](https://docs.google.com/spreadsheets/d/12Wwp48ZJVux26rCotx43lzdWmVL54JinsNnLIV3jnyM/edit?usp=sharing).
Ele já possui a estrutura padrão que utilizamos para dicionários.

### 7. Subir tudo no Google Cloud

Tudo pronto! Agora só falta subir para o Google Cloud e enviar para
revisão. Para isso, vamos usar o cliente `basedosdados` (disponível em Python) que facilita as configurações e etapas do processo.

!!! Info "Como existe um custo para o armazenamento no storage, para finalizar essa etapa vamos precisar te disponibilizar uma api_key especifica para voluntários para subir os dados em nosso ambiente de desenvolvimento. Assim, entre em nosso [canal no Discord](https://discord.gg/huKWpsVYx4) e nos chame em 'quero-contribuir'"

#### Configure suas credenciais localmente
  **7.1** No seu terminal instale nosso cliente: `pip install basedosdados`.
  **7.2** Rode `import basedosdados as bd` no python e siga o passo a passo para configurar localmente com as credenciais de seu projeto no Google Cloud. Preencha as informações conforme a seguir:
```
    * STEP 1: y
    * STEP 2: basedosdados-dev  (colocar o .json passado pela equipe da bd na pasta credentials)
    * STEP 3: y
    * STEP 4: basedosdados-dev
    * STEP 5: https://api.basedosdados.org/api/v1/graphql
```
#### Suba os arquivos na Cloud
Os dados vão passar por 3 lugares no Google Cloud:

  * **Storage**: também chamado de GCS é o local onde serão armazenados o arquivos "frios" (arquiteturas, dados, arquivos auxiliares).
  * **BigQuery-DEV-Staging**: tabela que conecta os dados do storage ao projeto basedosdados-dev no bigquery
  * **BigQuery-DEV-Produção**: tabela utilizada paras teste e tratamento via SQL do conjunto de dados

**7.3** Crie a tabela no *bucket do GCS* e *BigQuey-DEV-staging*, usando a API do Python, da seguinte forma:

    ```python
    import basedosdados as bd

    tb = bd.Table(
      dataset_id='<dataset_id>',
      table_id='<table_id>')

    tb.create(
        path='<caminho_para_os_dados>',
        if_table_exists='raise',
        if_storage_data_exists='raise',
    )
    ```

    Os seguintes parâmetros podem ser usados:


    - `path` (obrigatório): o caminho completo do arquivo no seu computador, como: `/Users/<seu_usuario>/projetos/basedosdados/mais/bases/[DATASET_ID]/output/microdados.csv`.


    !!! Tip "Caso seus dados sejam particionados, o caminho deve apontar para a pasta onde estão as partições. No contrário, deve apontar para um arquivo `.csv` (por exemplo, microdados.csv)."

    - `force_dataset`: comando que cria os arquivos de configuração do dataset no BigQuery.
        - _True_: os arquivos de configuração do dataset serão criados no seu projeto e, caso ele não exista no BigQuery, será criado automaticamente. **Se você já tiver criado e configurado o dataset, não use esta opção, pois irá sobrescrever arquivos**.
        - _False_: o dataset não será recriado e, se não existir, será criado automaticamente.
    - `if_table_exists` : comando utilizado caso a **tabela já exista no BQ**:
        - _raise_: retorna mensagem de erro.
        - _replace_: substitui a tabela.
        - _pass_: não faz nada.

    - `if_storage_data_exists`: comando utilizado caso os **dados já existam no Google Cloud Storage**:
        - _raise_: retorna mensagem de erro
        - _replace_: substitui os dados existentes.
        - _pass_: não faz nada.

    !!! Info "Se o projeto não existir no BigQuery, ele será automaticamente criado"

  Consulte também nossa [API](../api_reference_cli) para mais detalhes de cada método.

**7.4** Crie os arquivos .sql e schema.yml a partir da tabela de arquitetura seguindo essa [documentação](https://github.com/basedosdados/pipelines/wiki/Fun%C3%A7%C3%A3o-%60create_yaml_file()%60)
!!! Tip "Caso você precise, nesse momento você pode alterar a consulta em SQL para realizar tratamentos finais a partir da tabela `staging`, pode incluir coluna, remover coluna, fazer operações algébricas, substituir strings, etc. O SQL é o limite!"

**7.5** Rode e teste os modelos localmente seguindo essa [documentação](https://github.com/basedosdados/pipelines/wiki/Testar-modelos-dbt-localmente)

**7.6** Suba os metadados da tabela no site:
!!! Info "Por enquanto apenas a equipe dados tem permissões de subir os metadados da tabela no site, por isso será necessário entrar em contato conosco. Já estamos trabalhando para que, num futuro próximo, os voluntários também possam atualizar dados no site."

**7.7** Suba os arquivos auxiliares:
    ```python
    st = bd.Storage(
      dataset_id = <dataset_id>,
      table_id = <table_id>)

    st.upload(
      path='caminho_para_os_arquivos_auxiliares',
      mode = 'auxiliary_files',
      if_exists = 'raise')
    ```

### 8. Enviar tudo para revisão

Ufa, é isso! Agora só resta enviar tudo para revisão no
[repositório](https://github.com/basedosdados/queries-basedosdados) da Base dos Dados.

1. Clone o nosso [repositório](https://github.com/basedosdados/queries-basedosdados) localmente.
2. Dê um `cd` para a pasta local do repositório e abra uma nova branch com `git checkout -b [dataset_id]`. Todas as adições e modificações serão incluídas nessa _branch_.
3. Para cada tabela nova incluir o arquivo com nome `table_id.sql` na pasta `queries-basedosdados/models/dataset_id/` copiando as queries que você desenvolveu no passo 7.
4. Incluir o arquivo schema.yaml desenvolvido no passo 7
5. Caso seja um dataset novo, incluir o dataset conforme as instruções do arquivo `queries-basedosdados/dbt_project.yaml` (não se esqueça de seguir a ordem alfabética pra não bagunçar a organização)
6. Inclua o seu código de captura e limpeza em na pasta `queries-basedosdados/models/dataset_id/code`
7. Agora é só publicar a branch, abrir o PR com as labels 'table-approve' e marcar a equipe dados para correção

**E agora?** Nossa equipe irá revisar os dados e metadados submetidos
via Github. Podemos entrar em contato para tirar dúvidas ou solicitar
mudanças no código. Quando tudo estiver OK, fazemos um _merge_ do seu
*pull request* e os dados são automaticamente publicados na nossa
plataforma!
