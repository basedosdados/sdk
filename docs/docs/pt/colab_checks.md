# Colaborando com testes na BD

Para manter a qualidade dos bases de dados presentes na BD, nós contamos com um
conjunto de checagens automáticas que são realizadas durante a inserção e
atualização de cada base. Essas checagens são necessárias, mas não suficientes
para garantir a qualidade dos dados. Elas realizam consultas basicas, como se a
tabela existe ou se tem colunas totalmente nulas.

Você pode colaborar com a BD aumentando a cobertura dos testes, diminuindo assim
o trabalho de revisão dos dados. Para isso basta criar consultas que testem a
qualidade dos dados em SQL, como as seguintes:

- Verificar se colunas com proporção possuem valores entre 0 e 100
- Verificar se colunas com datas seguem o padrão YYYY-MM-DD HH:MM:SS

<!----------------------------------------------------------------------------->

## Qual o procedimento?

Incluir testes de dados deve seguir o fluxo de trabalho:

- [Colaborando com testes na BD](#colaborando-com-testes-na-bd)
  - [Qual o procedimento?](#qual-o-procedimento)
  - [1. Informe seu interesse](#1-informe-seu-interesse)
  - [2. Escreva sua consulta](#2-escreva-sua-consulta)
  - [3. Submeta sua consulta](#3-submeta-sua-consulta)

!!! Tip "Sugerimos que entre em nosso [canal no Discord](https://discord.gg/huKWpsVYx4) para tirar dúvidas e interagir com outros(as) colaboradores(as)! :)"

<!----------------------------------------------------------------------------->

## 1. Informe seu interesse

Converse conosco no bate-papo da infra no Discord. Caso não tenha uma sugestão de melhoria podemos procurar alguma consulta que ainda não foi escrita.

<!----------------------------------------------------------------------------->

## 2. Escreva sua consulta

Faça um fork do repositório da [Base dos Dados](https://github.com/basedosdados/mais/tree/master).
Em seguida adicione novas consultas e suas respectivas funções de execução nos arquivos
[checks.yaml](https://github.com/basedosdados/mais/blob/master/.github/workflows/data-check/checks.yaml)
e [test_data.py](https://github.com/basedosdados/mais/blob/master/.github/workflows/data-check/test_data.py).

As consultas são escritas em um arquivo YAML com `Jinja` e SQL, da forma:

```yaml
test_select_all_works:
  name: Check if select query in {{ table_id }} works
  query: |
    SELECT NOT EXISTS (
            SELECT *
        FROM `{{ project_id_staging }}.{{ dataset_id }}.{{ table_id }}`
    ) AS failure
```

E executadas como testes do pacote `pytest`:

```python
def test_select_all_works(configs):
    result = fetch_data("test_select_all_works", configs)
    assert result.failure.values == False
```

Não se assuste caso não conheça algo da sintaxe acima, podemos lhe ajudar durante
o processo. Note que os valores entre chaves são variáveis contidas em arquivos
`table_config.yaml`, que contém metadados das tabelas. Logo a escrita de consulta
é limitada pelos metadados existentes. Recomendamos consultar estes arquivos
no diretório das [bases](https://github.com/basedosdados/mais/tree/master/bases).

<!----------------------------------------------------------------------------->

## 3. Submeta sua consulta

Por fim realize um pull request para o repositório principal para que seja realizada uma revisão da consulta.
