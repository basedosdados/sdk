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
- Documentação (Docstrings)
- Testes
