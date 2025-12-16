# Contribua

Primeiro, leia o [guia principal](../CONTRIBUTING.md)

## Requisitos

- [uv](https://docs.astral.sh/uv/getting-started/installation/)

Depois de clonar o repositório entre na pasta local do repositório usando `cd sdk/python-package` faça o setup do ambiente de desenvolvimento:

```sh
uv sync
```

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
