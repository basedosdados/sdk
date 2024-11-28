# Python Package

## Requisitos

- `pyenv`: Para gerenciar versões do python
- `poetry`: Gerenciador de dependências

## Setup

Clone o repositório:

```sh
git clone git@github.com:basedosdados/sdk.git
```

Entre na pasta local do repositório usando `cd sdk/python-package` e faça o setup do ambiente de desenvolvimento:

Ative a versão do python:

```sh
pyenv shell 3.10
```

Crie a env:

```sh
poetry env use 3.10
```

> Se a env já existe, ative com `poetry shell`

Instale as dependências para desenvolvimento:

```sh
poetry install --with dev --all-extras --no-root
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

1. Fazer o pull da branch:

   ```bash
   git pull origin [python-version]
   ```

   Onde `[python-version]` é a branch da nova versão do pacote.

2. Editar `pyproject.toml`:

   O arquivo `pyproject.toml` contém, entre outras informações, a versão do pacote em python da **BD**. Segue excerto do arquivo:

   ```toml
   description = "Organizar e facilitar o acesso a dados brasileiros através de tabelas públicas no BigQuery."
   homepage = "https://github.com/base-dos-dados/bases"
   license = "MIT"
   name = "basedosdados"
   packages = [
     {include = "basedosdados"},
   ]
   readme = "README.md"
   repository = "https://github.com/base-dos-dados/bases"
   version = "1.6.1-beta.2"
   ```

   O campo `version` deve ser alterado para o número da versão sendo lançada.

3. Push para branch:

   ```sh
   git push origin [python-version]
   ```

4. Publicação do pacote no PyPI (exige usuário e senha):
   Para publicar o pacote no PyPI, use:

   ```sh
   poetry version [python-version]
   poetry publish --build
   ```

5. Faz merge da branch para a master
6. Faz release usando a UI do GitHub
7. Atualizar versão do pacote usada internamente
