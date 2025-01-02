
# Stata

Esta API é composta por módulos para **requisição de dados**: para aquele(as) que desejam
  somente consultar os dados e metadados do nosso projeto (ou qualquer outro
  projeto no Google Cloud).

!!! Info "Toda documentação do código abaixo está em inglês"

## Módulos (Requisição de dados)

Se é a sua primeira vez utilizando o pacote, digite ```db basedosdados``` e confirme novamente se as etapas acima foram concluídas com sucesso.

O pacote contém 7 comandos, conforme suas funcionalidades descritas abaixo:

| __Comando__               | __Descrição__                                                                  |
|---------------------------|--------------------------------------------------------------------------------|
| `bd_download`             | baixa dados da Base dos Dados (BD).                                           |
| `bd_read_sql`             | baixa tabelas da BD usando consultas específicas.                             |
| `bd_read_table`           | baixa tabelas da BD usando `dataset_id` e `table_id`.                         |
| `bd_list_datasets`        | lista o `dataset_id` dos conjuntos de dados disponíveis em `query_project_id`. |
| `bd_list_dataset_tables`  | lista `table_id` para tabelas disponíveis no `dataset_id` especificado.        |
| `bd_get_table_description`| mostra a descrição completa da tabela BD.                                     |
| `bd_get_table_columns`    | mostra os nomes, tipos e descrições das colunas na tabela especificada.        |

Cada comando tem um __help file__ de apoio, bastando abrir o help e seguir as instruções:

```
help [comando]
```
