## Pacote R
### Desenvolvimento via GitHub

Tenha certeza que você deseja colaborar! Às vezes, o que você gostaria de melhorar já foi observado ou já está em processo de manutenção. 
Verifique sempre as issues [abertas](https://github.com/basedosdados/sdk/labels/R) e [fechadas](https://github.com/basedosdados/sdk/issues?q=label%3AR+is%3Aclosed) sobre o pacote R. Se você deseja contribuir com o código no pacote .R, é necessário seguir os seguintes passos: 

### Propondo Mudanças
Abra uma Issue no [repositório](https://github.com/basedosdados/sdk/issues) `sdk` demonstrando para a equipe a existência de um bug, a necessidade de implementação de uma função nova ou até mesmo a melhoria plena de um processo ou função que já existe.
No caso de bugs, por favor, preencha a issue com um exemplo reprodutível [reprex](https://github.com/tidyverse/reprex).

### Fork o pacote e clone em sua máquina
`usethis::create_from_github("https://github.com/basedosdados/sdk/tree/master/r-package", fork = TRUE)`

### Instale as dependências de desenvolvimento 
Instale as dependências de desenvolvimento com o comando `devtools::install_dev_deps()` e posteriormente rode `devtools::check()`. Tenha certeza que o pacote passou no `devtools::check()`. Caso não passe, é uma boa ideia nos contactar via [Discord](https://discord.com/invite/huKWpsVYx4) ou via [site (https://basedosdados.org/contato).

### Crie uma nova git branch
Crie uma branch para seu Pull Request (PR) utilizando o comando `usethis::pr_init("breve_descricao_da_mudanca")`.

### Git Commit, Git Push e crie seu PR 
Realize as mudanças necessárias, commite na sua `branch` e crie seu PR utilizando o comando `usethis::pr_push()`. 
O título do seu PR deve seguir a seguinte forma: `[dados] minhas_mudancas.r` 
No corpo do PR deve conter `Fixes #numero da issue`

Agradecemos a colaboração e revisaremos seu código. 
Muito obrigadx! 
