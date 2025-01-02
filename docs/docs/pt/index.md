# Hello, world!

A missão da Base dos Dados é universalizar o uso de dados de qualidade
no Brasil e no mundo. Para isso, criamos uma ferramenta que te permite **acessar
recursos importantes de diversos conjuntos de dados públicos**, como:

- **Tabelas tratadas**: Tabelas completas, já tratadas e prontas
  para análise, disponíveis no nosso datalake público.

- **Dados originais**: Links com informações úteis para explorar mais
  sobre o conjunto de dados, como a fonte original e outros.

!!! Info "Temos um time de Dados e voluntários(as) de todo o Brasil que ajudam a limpar e manter as tabelas tratadas BD. [Saiba como fazer parte](colab_data)"

## Acessando tabelas tratadas BD

No nosso site você encontra a lista de todas as tabelas tratadas de
cada conjunto de dados. Apresentamos também informações importantes de todas
as tabelas, como a lista de colunas, cobertura temporal, periodicidade, entre
outras informações. Você pode consultar os dados das tabelas via:

### Download

Você pode baixar o arquivo CSV completo da tabela direto no site. Este
tipo de Consulta não está disponível para arquivos que ultrapassem 200 mil linhas.

### BigQuery (SQL)

O BigQuery é o um serviço de banco de dados em nuvem da
Google. Direto do navegador, você pode fazer consultas às tabelas
tratadas com:

- Rapidez: Mesmo queries muito longas demoram apenas minutos para serem processadas.

- Escala: O BigQuery escala magicamente para hexabytes se necessário.

- Economia: Todo usuário possui *1 TB gratuito por mês para consulta
  aos dados*.

<a
href="access_data_bq"
title="{{ lang.t('source.link.title')}}" class="md-button"
hover="background-color: var(--md-primary-fg-color--dark)">
    Aprenda
    :material-arrow-right:
</a>

### Pacotes

Os pacotes da Base dos Dados permitem o acesso ao *data lake* público
direto do seu computador ou ambiente de desenvolvimento.
<!-- Outra forma de acessar os recursos da BD é diretamente pelos endpoints, conforme
documentado em [BD Open API](https://basedosdados.org/openapi). -->
Os pacotes atualmente disponíveis são:

- **:material-language-python: Python**
- **:material-language-r: R**
- **Stata**

<a
href="access_data_packages"
title="{{ lang.t('source.link.title')}}" class="md-button"
hover="background-color: var(--md-primary-fg-color--dark)">
    Aprenda
    :material-arrow-right:
</a>

## Dicas para melhor uso dos dados

Nosso time de dados trabalha constantemente em desenvolver **melhores
padrões e metodologias** para facilitar o processo de análise de dados.
Separamos alguns materiais úteis para você entender melhor o que fazemos
e como tirar o melhor proveito dos dados:

- [Cruzar dados de diferentes organizações de forma rápida](tutorial_join_tables)
- [Entender padrões de tabelas, conjuntos e variáveis](style_data)
