# Contribuindo

O repositório `sdk` contém o código base para os pacotes de **BD** em Python, R e Stata. Neste documento, detalhamos a configuração de ambiente necessária para contribuir em cada um dos nossos pacotes.

## Iniciando

Faça um fork do repositório e clone. Para mais detalhes veja o tutorial como contribuir para um [projeto no GitHub](https://docs.github.com/en/get-started/exploring-projects-on-github/contributing-to-a-project)

```sh
git clone git@github.com:USERNAME/sdk.git
```

> [!NOTE]
> Não esqueça de criar sua branch para fazer alterações

Acesso o guia de contribuição para cada pacote:

- [Python Package](./python-package/README.md)
- [R Package](./r-package/README.md)
- [Stata Package](./stata-package/README.md)

## Processo de contribuição

1. Abra uma branch com o nome issue-<X>
2. Faça as modificações necessárias
3. Suba o Pull Request
4. O nome do PR deve seguir o padrão
   `[infra] <titulo explicativo>`

## O que uma modificação precisa ter

- Resolver um problema
- Lista de modificações efetuadas
  1. Mudei a função X para fazer Y
  2. Troquei o nome da variavel Z
- Referência aos issues atendidos ser possível
- Documentação e Docstrings
- Testes

## Documentação

Para rodar a documentação em `docs` localmente você precisa instalar as dependências [Python](./python-package/README.md)

Acesse a pasta `docs/` na raiz do projeto e execute

```bash
mkdocs serve # Acesse http://localhost:8000/
```

Atualize os docs adicionando ou editando os arquivos `.md` em `docs/`.

Para adicionar seu arquivo no sumário da documentação, adicione-o em `mkdocs.yml` sob a chave `nav`:

```yaml
nav:
  - Home:
      - Aprenda sobre a BD: index.md
      - BigQuery: access_data_bq.md
      - Pacotes: access_data_packages.md
      - Contribua: colab.md
      - [Seu novo título]: <seu_arquivo>.md
```
