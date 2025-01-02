# Manual de estilo

Nessa seção listamos todos os padrões do nosso manual de estilo e diretrizes de dados que usamos na Base dos Dados. Eles nos ajudam a manter os dados e metadados que publicamos com qualidade alta.

!!! Tip "Você pode usar o menu esquerdo para navegar pelos diferentes tópicos dessa página."
<!-- **Resumo**:

- [Nomeação de bases e tabelas](#nomeação-de-bases-e-tabelas)
- [Formatos de tabelas](#formatos-de-tabelas)
- [Nomeação de variáveis](#nomeacao-de-variaveis)
- [Ordenamento de variáveis](#ordenamento-de-variáveis)
- [Tipos de variáveis](#tipos-de-variaveis)
- [Unidades de medida](#unidades-de-medida)
- [Quais variáveis manter, quais adicionar e quais remover](#quais-variáveis-manter-quais-adicionar-e-quais-remover)
- [Cobertura temporal](#cobertura-temporal)
- [Limpando STRINGs](#limpando-strings)
- [Formatos de valores](#formatos-de-valores)
- [Particionamento de tabelas](#particionamento-de-tabelas)
- [Número de bases por _pull request_](#número-de-bases-por-pull-request)
- [Dicionários](#dicionarios)
- [Diretórios](#diretorios) -->

---

## Nomeação de bases e tabelas

### Conjuntos de dados (`dataset_id`)

Nomeamos conjuntos no formato `<organization_id\>_<descrição\>`, onde
`organization_id` segue por padrão a **abrangência geográfica da
organização** que publica o conjunto:

|           | organization_id                         |
|-----------|----------------------------------------------|
| Mundial   | mundo_<organizacao\>                         |
| Federal   | <sigla_pais\>\_<organizacao\>                         |
| Estadual  | <sigla_pais\>\_<sigla_uf\>\_<organizacao\>             |
| Municipal | <sigla_pais\>\_<sigla_uf\>\_<cidade\>\_<organizacao\>   |

* `sigla_pais` e `sigla_uf` são sempre 2 letras minúsculas;
* `organizacao` é o nome ou sigla (de preferência) da organização que
  publicou os dados orginais (ex: `ibge`, `tse`, `inep`).
* `descricao` é uma breve descrição do conjunto de dados, que pode ser

Por exemplo, o conjunto de dados do PIB do IBGE tem como `dataset_id`: `br_ibge_pib`

!!! Tip "Não sabe como nomear a organização?"
    Sugerimos que vá no site da mesma e veja como ela se autodenomina (ex: DETRAN-RJ seria `br_rj_detran`)

### Tabelas

Nomear tabelas é algo menos estruturado e, por isso, requer bom senso. Mas temos algumas regras:

- Se houver tabelas para diferentes entidades, incluir a entidade no começo do nome. Exemplo: `municipio_valor`, `uf_valor`.
- Não incluir a unidade temporal no nome. Exemplo: nomear `municipio`, e não `municipio_ano`.
- Deixar nomes no singular. Exemplo: `escola`, e não `escolas`.
- Nomear de `microdados` as tabelas mais desagregadas. Em geral essas tem dados a nível de pessoa ou transação.

### Exemplos de `dataset_id.table_id`

|           |                                           |                                                     |
|-----------|-------------------------------------------|-----------------------------------------------------|
| Mundial   | `mundo_waze.alertas`                      | Dados de alertas do Waze de diferentes cidades.    |
| Federal   | `br_tse_eleicoes.candidatos`              | Dados de candidatos a cargos políticos do TSE.      |
| Federal   | `br_ibge_pnad.microdados`                 | Microdados da Pesquisa Nacional por Amostra de Domicílios produzidos pelo IBGE. |
| Federal   | `br_ibge_pnadc.microdados`                | Microdados da Pesquisa Nacional por Amostra de Domicílios Contínua (PNAD-C) produzidos pelo IBGE. |
| Estadual  | `br_sp_see_docentes.carga_horaria`        | Carga horária anonimizado de docentes ativos da rede estadual de ensino de SP. |
| Municipal | `br_rj_riodejaneiro_cmrj_legislativo.votacoes` | Dados de votação da Câmara Municipal do Rio de Janeiro (RJ). |

## Formatos de tabelas

Tabelas devem, na medida do possível, estar no formato `long`, ao invés de `wide`.

## Nomeação de variáveis

Nomes de variáveis devem respeitar algumas regras:

- Usar ao máximo nomes já presentes no repositório. Exemplos: `ano`, `mes`, `id_municipio`, `sigla_uf`, `idade`, `cargo`, `resultado`, `votos`, `receita`, `despesa`, `preco`, etc.
- Respeitar padrões das tabelas de diretórios.
- Ser o mais intuitivo, claro e extenso possível.
- Ter todas letras minúsculas (inclusive siglas), sem acentos, conectados por `_`.
- Não incluir conectores como `de`, `da`, `dos`, `e`, `a`, `em`, etc.
- Só ter o prefixo `id_` quando a variável representar chaves primárias de entidades (que eventualmente teriam uma tabela de diretório).
    - Exemplos que tem: `id_municipio`, `id_uf`, `id_escola`, `id_pessoa`.
    - Exemplos que não tem: `rede`, `localizacao`.
    - **Importante**: quando a base está em inglês id vira um sufixo
- Só ter sufixos de entidade quando a entidade da coluna for diferente da entidade da tabela.
    - Exemplos que tem: numa tabela com entidade `pessoa`, uma coluna sobre PIB municipal se chamaria `pib_municipio`.
    - Exemplos que não tem: numa tabela com entidade `pessoa`, características da pessoa se chamariam `nome`, `idade`, `sexo`, etc.
- Lista de **prefixos permitidos**
    - `nome_`,
    - `data_`,
    - `quantidade_`,
    - `proporcao_` (variáveis de porcentagem 0-100%),
    - `taxa_`,
    - `razao_`,
    - `indice_`,
    - `indicador_` (variáveis do tipo booleano),
    - `tipo_`,
    - `sigla_`,
    - `sequencial_`.
- Lista de **sufixos comuns**
    - `_pc` (per capita)

## Ordenamento de variáveis

A ordem de variáveis em tabelas é padronizada para manter uma consistência no repositório. Nossas regras são:

- Chaves primárias à esquerda, em ordem descendente de abrangência;
- No meio devem estar variáveis qualitativas da linha;
- As últimas variáveis devem ser os valores quantitativos em ordem crescente de relevância;
- Exemplo de ordem: `ano`, `sigla_uf`, `id_municipio`, `id_escola`, `rede`, `nota_ideb`;
- Dependendo da tabela, pode ser recomendado agrupar e ordenar variáveis por temas.

## Tipos de variáveis

Nós utilizamos algumas das opções de [tipos do BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types): `string`, `int64`, `float64`, `date`, `time`, `geography`.

Quando escolher:

- `string`:
    - Variáveis de texto
    - Chaves de variáveis categóricas com dicionário ou diretório
- `int64`:
    - Variáveis de números inteiros com as quais é possível fazer contas (adição, subtração)
    - Variáveis do tipo booleanas que preenchemos com 0 ou 1
- `float64`:
    - Variáveis de números com casas decimais com as quais é possível fazer contas (adição, subtração)
- `date`:
    - Variáveis de data no formato `YYYY-MM-DD`
- `time`:
    - Variáveis de tempo no formato `HH:MM:SS`
- `geography`:
    - Variáveis de geografia

## Unidades de medida

A regra é manter variáveis com suas unidades de medida originais listadas nesse [código](https://github.com/basedosdados/website/blob/master/ckanext-basedosdados/ckanext/basedosdados/validator/available_options/measurement_unit.py), com a exceção de variáveis financeiras onde convertermos moedas antigas para as atuais (e.g. Cruzeiro para Real).

Catalogamos unidades de medida em formato padrão na tabela de arquitetura. [Lista completa aqui](https://github.com/basedosdados/website/blob/master/ckanext-basedosdados/ckanext/basedosdados/validator/available_options/measurement_unit.py) Exemplos: `m`, `km/h`, `BRL`.

Para colunas financeiras deflacionadas, listamos a moeda com o ano base. Exemplo: uma coluna medida em reais de 2010 tem unidade `BRL_2010`.

Variáveis devem ter sempre unidades de medida com base 1. Ou seja, ter `BRL` ao invés de `1000 BRL`, ou `pessoa` ao invés de `1000 pessoas`. Essa informação, como outros metadados de colunas, são registradas na tabela de arquitetura da tabela.

## Quais variáveis manter, quais adicionar e quais remover

Mantemos nossas tabelas parcialmente [normalizadas](https://www.guru99.com/database-normalization.html), e temos regras para quais variáveis incluirmos em produção. Elas são:

- Remover variáveis de nomes de entidades que já estão em diretórios. Exemplo: retirar `municipio` da tabela que já inclui `id_municipio`.
- Remover variáveis servindo de partição. Exemplo: remover `ano` e `sigla_uf` se a tabela é particionada nessas duas dimensões.
- Adicionar chaves primárias principais para cada entidade já existente. Exemplo: adicionar `id_municipio` a tabelas que só incluem `id_municipio_tse`.
- Manter todas as chaves primárias que já vem com a tabela, mas (1) adicionar chaves relevantes (e.g. `sigla_uf`, `id_municipio`) e (2) retirar chaves irrelevantes (e.g. `regiao`).

## Cobertura temporal

Preencher a coluna `cobertura_temporal` nos metadados de tabela, coluna e chave (em dicionários) segue o seguinte padrão.

- Formato geral: `data_inicial(unidade_temporal)data_final`
    - `data_inicial` e `data_final` estão na correspondente unidade temporal.
        - Exemplo: tabela com unidade `ano` tem cobertura `2005(1)2018`.
        - Exemplo: tabela com unidade `mes` tem cobertura `2005-08(1)2018-12`.
        - Exemplo: tabela com unidade `semana` tem cobertura `2005-08-01(7)2018-08-31`.
        - Exemplo: tabela com unidade `dia` tem cobertura `2005-08-01(1)2018-12-31`.

- Regras para preenchimento
    - Metadados de tabela
        - Preencher no formato geral.
    - Metadados de coluna
        - Preencher no formato geral, exceto quando a `data_inicial` ou `data_final` sejam iguais aos da tabela. Nesse caso deixe vazio.
        - Exemplo: suponha que a cobertura da tabela seja `2005(1)2018`.
            - Se uma coluna aparece só em 2012 e existe até 2018, preenchemos sua cobertura como `2012(1)`.
            - Se uma coluna desaparece em 2013, preenchemos sua cobertura como `(1)2013`.
            - Se uma coluna existe na mesma cobertura temporal da tabela, preenchemos sua cobertura como `(1)`.
    - Metadados de chave
        - Preencher no mesmo padrão de colunas, mas a referência sendo a coluna correspondente, e não a tabela.

## Limpando STRINGs

- Variáveis categóricas: inicial maiúscula e resto minúsculo, com acentos.
- STRINGs não-estruturadas: manter igual aos dados originais.

## Formatos de valores

- Decimal: formato americano, i.e. sempre `.` (ponto) ao invés de `,` (vírgula).
- Data: `YYYY-MM-DD`
- Horário (24h): `HH:MM:SS`
- Datetime ([ISO-8601](https://en.wikipedia.org/wiki/ISO_8601)): `YYYY-MM-DDTHH:MM:SS.sssZ`
- Valor nulo: `""` (csv), `NULL` (Python), `NA` (R), `.` ou `""` (Stata)
- Proporção/porcentagem: entre 0-100

## Particionamento de tabelas

### **O que é particionamento e qual seu objetivo ?**

De forma resumida, particionar uma tabela é dividi-la em vários blocos/partes. O objetivo central é diminuir os custos financeiros e aumentar a perfomance, visto que, quanto maior o volume de dados, consequentemente será maior o custo de armazenamento e consulta.

A redução de custos e o aumento de perfomance acontece, principalmente, porque a partição permite a reorganização do conjunto de dados em pequenos **blocos agrupados**. Na prática, realizando o particionamento, é possível evitar que uma consulta percorra toda a tabela só para trazer um pequeno recorte de dados.

Um exemplo prático da nossa querida RAIS:

- Sem utilizar filtro de partição:

Para esse caso o Bigquery varreu todas (*) as colunas e linhas do conjunto. Vale salientar que esse custo ainda não é tão grande, visto que a base já foi particionada. Caso esse conjunto não tivesse passado pelo processo de particionamento, essa consulta custaria muito mais dinheiro e tempo, já que se trata de um volume considerável de dados.

![image](https://user-images.githubusercontent.com/58278652/185815101-68ed5797-fff8-4968-84e2-e6a47bba58d0.png)

- Com filtro de partição:

Aqui, filtramos pelas colunas particionadas `ano` e `sigla_uf`. Dessa forma, o Bigquery só **consulta** e **retorna** os valores da pasta **ano** e da subpasta **sigla_uf**.

![image](https://user-images.githubusercontent.com/58278652/185815135-fb012a2c-535b-457e-af2a-7984961168b3.png)

### **Quando particionar uma tabela?**

A primeira pergunta que surge quando se trata de particionamento é: _a partir de qual quantidade de linhas uma tabela deve ser particionada?_ A documentação do [GCP ](https://cloud.google.com/bigquery/docs/partitioned-tables?hl=pt-br) não define uma quantidade _x_ ou _y_  de linhas que deve ser particionada. O ideal é que as tabelas sejam particionadas, com poucas exceções. Por exemplo, tabelas com menos de 10.000 linhas, que não receberão mais ingestão de dados, não tem um custo de armazenamento e processamento altos e, portanto, não há necessidade de serem particionadas.

### **Como particionar uma tabela?**

Se os dados estão guardados localmente, é necessário:

1. Criar as pastas particionadas na sua pasta de `/output`, na linguagem que você estiver utilizando.

Exemplo de uma tabela particionada por `ano` e `mes`, utilizando `python`:

```python
for ano in [*range(2005, 2020)]:
  for mes in [*range(1, 13)]:
    particao = output + f'table_id/ano={ano}/mes={mes}'
    if not os.path.exists(particao):
      os.makedirs(particao)
```
2. Salvar os arquivos particionados.

```python
for ano in [*range(2005, 2020)]:
  for mes in [*range(1, 13)]:
    df_particao = df[df['ano'] == ano].copy() # O .copy não é necessário é apenas uma boa prática
    df_particao = df_particao[df_particao['mes'] == mes]
    df_particao.drop(['ano', 'mes'], axis=1, inplace=True) # É preciso excluir as colunas utilizadas para partição
    particao = output + f'table_id/ano={ano}/mes={mes}/tabela.csv'
    df_particao.to_csv(particao, index=False, encoding='utf-8', na_rep='')
```

Exemplos de tabelas particionadas em `R`:

- [PNADC](https://github.com/basedosdados/mais/blob/master/bases/br_ibge_pnadc/code/microdados.R)
- [PAM](https://github.com/basedosdados/mais/blob/master/bases/br_ibge_pam/code/permanentes_usando_api.R)

Exemplo de como particionar uma tabela em `SQL`:

```sql
CREATE TABLE `dataset_id.table_id` as (
    ano  INT64,
    mes  INT64,
    col1 STRING,
    col1 STRING
) PARTITION BY ano, mes
OPTIONS (Description='Descrição da tabela')
```

### **Regras importantes de particionamento.**

- Os tipos de colunas que o BigQuery aceita como partição são:

  * **Coluna de unidade de tempo**: as tabelas são particionadas com base em uma coluna de `TIMESTAMP`, `DATE` ou `DATETIME`.
  * **Tempo de processamento**: as tabelas são particionadas com base no carimbo de `data/hora` quando o BigQuery processa os dados.
  * **Intervalo de números inteiros**: as tabelas são particionadas com base em uma coluna de números inteiros.

- Os tipos de colunas que o BigQuery **não** aceita como partição são: `BOOL`, `FLOAT64`, `BYTES`, etc.

- O BigQuery aceita no máximo 4.000 partições por tabela.

- Aqui na BD as tabelas geralmente são particionadas por: `ano`, `mes`, `trimestre` e `sigla_uf`.

- Note que ao particionar uma tabela é preciso excluir a coluna correspondente. Exemplo: é preciso excluir a coluna `ano` ao particionar por `ano`.


## Número de bases por _pull request_

Pull requests no Github devem incluir no máximo um conjunto, mas pode incluir mais de uma base. Ou seja, podem envolver uma ou mais tabela dentro do mesmo conjunto.

## Dicionários

- Cada base inclui somente um dicionário (que cobre uma ou mais tabelas).
- Para cada tabela, coluna, e cobertura temporal, cada chave mapeia unicamente um valor.
- Chaves não podem ter valores nulos.
- Dicionários devem cobrir todas as chaves disponíveis nas tabelas originais.
- Chaves só podem possuir zeros à esquerda quando o número de dígitos da variável tiver significado. Quando a variável for `enum` padrão, nós excluimos os zeros à esquerda.
    - Exemplo: mantemos o zero à esquerda da variável `br_bd_diretorios_brasil.cbo_2002:cbo_2002`, que tem seis dígitos, pois o primeiro dígito `0` significa a categoria ser do `grande grupo = "Membros das forças armadas, policiais e bombeiros militares"`.
    - Para outros casos, como por exemplo `br_inep_censo_escolar.turma:etapa_ensino`, nós excluimos os zeros à esquerda. Ou seja, mudamos `01` para `1`.
- Valores são padronizados: sem espaços extras, inicial maiúscula e resto minúsculo, etc.

### **Como preencher os metadados da tabela dicionário?**
- Não preencher o *`spatial_coverage`* (`cobertura_espacial`), ou seja, deixar o campo vazio.
- Não preencher o *`temporal_coverage`* (`cobertura_temporal`), ou seja, deixar o campo vazio.
- Não preencher o *`observation_level`* (`nivel_observacao`), ou seja, deixar o campo vazio.

## Diretórios

Diretórios são as pedras fundamentais da estrutura do nosso _datalake_. Nossas regras para gerenciar diretórios são:

- Diretórios representam _entidades_ do repositório que tenham chaves primárias (e.g. `uf`, `município`, `escola`) e unidades de data-tempo (e.g. `data`, `tempo`, `dia`, `mes`, `ano`).
- Cada tabela de diretório tem ao menos uma chave primária com valores únicos e sem nulos. Exemplos: `municipio:id_municipio`, `uf:sigla_uf`.
- Nomes de variáveis com prefixo `id_` são reservadas para chaves
  primárias de entidades.

Veja todas as [tabelas já disponíveis aqui.](https://basedosdados.org/dataset?organization=br-bd&order_by=score&q=%22diret%C3%B3rios%22)

### **Como preencher os metadados das tabelas de diretório?**
- Preencher o *`spatial_coverage`* (`cobertura_espacial`), que é a máxima unidade espacial que a tabela cobre. Exemplo: sa.br, que significa que o nível de agregação espacial da tabela é o Brasil.
- Não preencher o *`temporal_coverage`* (`cobertura_temporal`), ou seja, deixar o campo vazio.
- Preencher o *`observation_level`* (`nivel_observacao`), que consiste no nível de observação da tabela, ou seja, o que representa cada linha.
- Não preencher o *`temporal_coverage`* (`cobertura_temporal`) das colunas da tabela, ou seja, deixar o campo vazio.

## Fontes Originais

O campo se refere aos dados na fonte original, que ainda não passaram pela metodologia de tratamento da Base dos Dados, ou seja, nosso `_input_`. Ao clicar nele, a ideia é redirecionar o usuário para a página da fonte original dos dados. As regras para gerenciar as Fontes Originais são:

- Incluir o nome do link externo que leva à fonte original. Como padrão, esse nome deve ser da organização ou do portal que armazenena os dados. Exemplos: `Sinopses Estatísticas da Educação Básica: Dados Abertos do Inep`, `Penn World Tables: Groningen Growth and Development Centre`.
- Preencher os metadados de Fontes Originais: Descrição, URL, Língua, Tem Dados Estruturados, Tem uma API, É de Graça, Requer Registro, Disponibilidade, Requer IP de Algum País, Tipo de Licença, Cobertura Temporal, Cobertura Espacial e Nível da Observação.

## **Pensou em melhorias para os padrões definidos?**

Abra um [issue no nosso Github](https://github.com/basedosdados/mais/labels/docs) ou mande uma mensagem no [Discord](https://discord.gg/huKWpsVYx4) para conversarmos :)
