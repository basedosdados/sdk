# Contribua

Primeiro, leia o [guia principal](../CONTRIBUTING.md)

## Requisitos

- [uv](https://docs.astral.sh/uv/getting-started/installation/)

Depois de clonar o repositório entre na pasta local do repositório usando `cd sdk/python-package` faça o setup do ambiente de desenvolvimento:

```sh
uv sync
```

## Desenvolva uma nova feature

1. Abra uma branch com o nome issue-<X>
2. Faça as modificações necessárias
3. Suba o Pull Request apontando para a branch `python-next-minor` ou `python-next-patch`.
   Sendo, minor e patch referentes ao bump da versão: v1.5.7 --> v\<major>.\<minor>.\<patch>.
4. O nome do PR deve seguir o padrão
   `[infra] <titulo explicativo>`

## O que uma modificação precisa ter

- Resolver o problema
- Lista de modificações efetuadas
  1. Mudei a função X para fazer Y
  2. Troquei o nome da variavel Z
- Referência aos issues atendidos
- Documentação e Docstrings
- Testes

## Versionamento

**Para publicar uma nova versão do pacote é preciso seguir os seguintes passos:**

1. [Atualize a versão do pacote](https://docs.astral.sh/uv/guides/package/#updating-your-version)

2. Push para branch:

   ```sh
   git push origin [python-version]
   ```

3. Publicação do pacote no PyPI (exige usuário e senha):

   ```sh
   uv publish
   ```

5. Faz merge da branch para a master
6. Faz release usando a UI do GitHub
7. Atualizar versão do pacote usada internamente
