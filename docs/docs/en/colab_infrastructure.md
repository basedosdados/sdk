# DB Infrastructure

Our infrastructure team ensures that all packages and pipelines are working optimally for the public. We use Github to manage all code and keep it organized, where you can find issues for new features, bugs, and improvements we're working on.

## How our infrastructure works

Our infrastructure consists of 3 main fronts:

- [**Data ingestion system**](#data-ingestion-system): from upload to production deployment;
- [**Access packages**](#access-packages)
- [**Website**](#website): Front-end, Back-end, and APIs.

Currently, it's possible to collaborate on all fronts, with emphasis on developing checks and balances and website updates.

!!! Tip "We suggest joining our [Discord channel](https://discord.gg/huKWpsVYx4) to ask questions and interact with other contributors! :)"


## Data ingestion system

The system has development (`basedosdados-dev`), staging (`basedosdados-staging`), and production (`basedosdados`) environments in BigQuery. The data upload processes are detailed in the image below, with some of them being automated via Github Actions.

![](images/bd_infra_diagram.png)

We explain the system's operation in more detail [on our blog](https://dev.to/basedosdados/como-funciona-o-sistema-de-insercao-de-dados-na-bd-25dk).

### How to contribute?

- Improving system documentation here :)
- [Creating automatic data and metadata quality checks (in Python)](https://github.com/basedosdados/mais/issues/376)
- [Creating new issues and improvement suggestions](https://github.com/basedosdados/mais/issues/new/choose)

## Access packages

The *datalake* access packages are constantly being improved, and you can collaborate with us on new features, bug fixes, and much more.

### How to contribute?

- [Explore Python package issues](https://github.com/basedosdados/mais/labels/python)
- [Explore R package issues](https://github.com/basedosdados/mais/labels/R)
- [Help develop the Stata package](https://github.com/basedosdados/mais/pull/754)

## Website

Our website is developed in [Next.js](https://nextjs.org/learn/basics/create-nextjs-app) and consumes a CKAN metadata API. The site's code is also on our [Github](https://github.com/basedosdados/website).

### How to contribute?

- [Improve site UX (Next, CSS, HTML)](https://github.com/basedosdados/website#editando-html)
- [Help with open BE, FE, or API issues](https://github.com/basedosdados/website/issues)
- [Create new issues and improvement suggestions](https://github.com/basedosdados/website/issues/new)
