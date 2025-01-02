# Hello, world!

La misión de Base de los Datos es universalizar el uso de datos de calidad
en Brasil y el mundo. Para ello, creamos una herramienta que te permite **acceder a
recursos importantes de diversos conjuntos de datos públicos**, como:

- **Tablas procesadas BD+**: Tablas completas, ya procesadas y listas
  para análisis, disponibles en nuestro datalake público.

- **Datos originales**: Enlaces con información útil para explorar más
  sobre el conjunto de datos, como la fuente original y otros.

!!! Info "Tenemos un equipo de Datos y voluntarios(as) de todo Brasil que ayudan a limpiar y mantener las tablas procesadas. [Aprende cómo formar parte](colab_data.md)"

## Accediendo a tablas procesadas BD+

En nuestro sitio encontrarás la lista de todas las tablas procesadas de
cada conjunto de datos. También presentamos información importante de todas
las tablas, como la lista de columnas, cobertura temporal, periodicidad, entre
otra información. Puedes consultar los datos de las tablas vía:

### Descarga

Puedes descargar el archivo CSV completo de la tabla directamente en el sitio. Este
tipo de consulta no está disponible para archivos que excedan 200 mil filas.

### BigQuery (SQL)

BigQuery es un servicio de base de datos en la nube de
Google. Directamente desde el navegador, puedes realizar consultas a las tablas
procesadas con:

- Rapidez: Incluso las consultas muy largas tardan solo minutos en procesarse.

- Escala: BigQuery escala mágicamente a hexabytes si es necesario.

- Economía: Cada usuario tiene *1 TB gratuito por mes para consultar
  los datos*.

<a
href="access_data_bq"
title="{{ lang.t('source.link.title')}}" class="md-button"
hover="background-color: var(--md-primary-fg-color--dark)">
    Aprende
    :material-arrow-right:
</a>

### Paquetes

Los paquetes de Base de los Datos permiten el acceso al *data lake* público
directamente desde tu computadora o entorno de desarrollo.
<!-- Otra forma de acceder a los recursos de BD es directamente a través de los endpoints, como está
documentado en [BD Open API](https://basedosdados.org/openapi). -->
Los paquetes actualmente disponibles son:

- **:material-language-python: Python**
- **:material-language-r: R**
- **Stata**

<a
href="access_data_packages"
title="{{ lang.t('source.link.title')}}" class="md-button"
hover="background-color: var(--md-primary-fg-color--dark)">
    Aprende
    :material-arrow-right:
</a>

## Consejos para un mejor uso de los datos

Nuestro equipo de datos trabaja constantemente en desarrollar **mejores
estándares y metodologías** para facilitar el proceso de análisis de datos.
Hemos separado algunos materiales útiles para que entiendas mejor lo que hacemos
y cómo sacar el mejor provecho de los datos:

- [Cruzar datos de diferentes organizaciones de forma rápida](tutorial_join_tables.md)
- [Entender patrones de tablas, conjuntos y variables](style_data.md)
