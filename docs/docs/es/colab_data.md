# Suba datos en BD

## ¿Por qué mi organización debería subir datos a BD?

- **Capacidad de cruzar sus bases con datos de diferentes
  organizaciones** de forma simple y fácil. Ya hay cientos de conjuntos
  de datos públicos de las mayores organizaciones de Brasil y del mundo presentes
  en nuestro *data lake*.

- **Compromiso con la transparencia, calidad de los datos y
  desarrollo de mejores investigaciones, análisis y soluciones** para la
  sociedad. No solo democratizamos el acceso a datos abiertos, sino también datos
  de calidad. Tenemos un equipo especializado que revisa y garantiza la calidad de los
  datos añadidos al *data lake*.

- **Participación en una comunidad que crece cada vez más**: miles
  de periodistas, investigadores(as), desarrolladores(as), ya utilizan y
  siguen la Base de los Datos.
  <!-- TODO: Colocar aquí el enlace de nuestro panel de audiencia cuando esté listo :) -->

## Paso a paso para subir datos

¿Quieres subir datos a BD y ayudarnos a construir este repositorio?
¡*Maravilloso!* Organizamos todo lo que necesitas en el manual a continuación en 8 pasos

Para facilitar la explicación, seguiremos un ejemplo ya listo con datos de [RAIS](https://basedosdados.org/es/dataset/3e7c4d58-96ba-448e-b053-d385a829ef00).

!!! Tip "Puedes navegar por las etapas en el menú de la izquierda."
    ¡Sugerimos encarecidamente que te unas a nuestro [canal en
    Discord](https://discord.gg/huKWpsVYx4) para resolver dudas e
    interactuar con el equipo y otros(as) colaboradores(as)! 😉

### Antes de empezar

Algunos conocimientos son necesarios para realizar este proceso:

- **Python, R, SQL y/o Stata**: para crear los códigos de captura y limpieza de los datos.
- **Línea de comandos**: para configurar tu ambiente local
  y conexión con Google Cloud.
- **Github**: para subir tu código para revisión de
  nuestro equipo.

!!! Tip "¿No tienes alguna de estas habilidades, pero quieres colaborar?"
    Tenemos un equipo de datos que puede ayudarte, solo únete a [nuestro
    Discord](https://discord.gg/huKWpsVYx4) y envía un mensaje en #quiero-contribuir.

### ¿Cómo funciona el proceso?

- [1. Elegir la base y entender más de los datos](#1-elegir-la-base-y-entender-mas-de-los-datos) - primero necesitamos conocer lo que estamos tratando.
- [2. Descargar nuestra carpeta template](#2-descargar-nuestra-carpeta-template) - es hora de estructurar el trabajo a realizar
- [3. Completar las tablas de arquitectura](#3-completar-las-tablas-de-arquitectura) - es primordial definir la estructura de los datos antes de iniciar el tratamiento
- [4. Escribir código de captura y limpieza de datos](#4-escribir-codigo-de-captura-y-limpieza-de-datos) - ¡hora de poner manos a la obra!
- [5. (Si es necesario) Organizar archivos auxiliares](#5-si-es-necesario-organizar-archivos-auxiliares) - porque hasta los datos necesitan guías
- [6. (Si es necesario) Crear tabla diccionario](#6-si-es-necesario-crear-tabla-diccionario) - momento de armar los diccionarios
- [7. Subir todo a Google Cloud](#7-subir-todo-a-google-cloud) - después de todo, es allí donde están los datos de BD
- [8. Enviar todo para revisión](#8-enviar-todo-para-revision) - ¡una mirada de nuestro equipo para garantizar que todo está listo para producción!

### 1. Elegir la base y entender más de los datos

Mantenemos la lista de conjuntos para voluntarios en nuestro [Github](https://github.com/orgs/basedosdados/projects/17/views/10). Para empezar a subir una base de tu interés, solo abre un [nuevo issue](https://github.com/basedosdados/queries-basedosdados/issues/new?assignees=&labels=&projects=&template=issue--novos-dados.md&title=%5Bdados%5D+%3Cdataset_id%3E) de datos. Si tu base (conjunto) ya está listada, solo marca tu usuario de Github como `assignee`

Tu primer trabajo es completar la información en el issue. Esta información te ayudará a entender mejor los datos y será muy útil para el tratamiento y el llenado de metadatos.

Cuando finalices esta etapa, llama a alguien del equipo de datos para que la información que has mapeado sobre el conjunto ya entre en nuestro sitio!

### 2. Descargar nuestra carpeta template

[Descarga aquí la carpeta
_template_](https://drive.google.com/drive/folders/1xXXon0vdjSKr8RCNcymRdOKgq64iqfS5?usp=sharing)
 y renómbrala como `<dataset_id>` (definido en el issue del [paso 1](#-1-Elegir-la-base-y-entender-mas-de-los-datos)). Esta carpeta template facilita y organiza todos los
pasos de aquí en adelante. Su
estructura es la siguiente:

- `<dataset_id>/`
    - `code/`: Códigos necesarios para **captura** y **limpieza** de los datos
    ([veremos más en el paso
    4](#4-escribir-codigo-de-captura-y-limpieza-de-datos)).
    - `input/`: Contiene todos los archivos con datos originales, exactamente
    como se descargaron de la fuente primaria. ([veremos más en el paso
    4](#4-escribir-codigo-de-captura-y-limpieza-de-datos)).
    - `output/`: Archivos finales, ya en el formato listo para subir a BD ([veremos más en el paso
    4](#4-escribir-codigo-de-captura-y-limpieza-de-datos)).
    - `tmp/`: Cualquier archivo temporal creado por el código en `/code` en el proceso de limpieza y tratamiento ([veremos más en el paso
    4](#4-escribir-codigo-de-captura-y-limpieza-de-datos)).
    - `extra/`
        - `architecture/`: Tablas de arquitectura ([veremos más en el paso 3](#3-completar-las-tablas-de-arquitectura)).
        - `auxiliary_files/`: Archivos auxiliares a los datos ([veremos más en el paso 5](#5-si-es-necesario-organizar-archivos-auxiliares)).
        - `dicionario.csv`: Tabla diccionario de todo el conjunto de datos ([veremos más en el paso 6](#6-si-es-necesario-crear-tabla-diccionario)).

!!! info "Solo la carpeta `code` será commitada para tu proyecto, los demás archivos existirán solo localmente o en Google Cloud."

### 3. Completar las tablas de arquitectura

Las tablas de arquitectura determinan **cuál es la estructura de
cada tabla de tu conjunto de datos**. Definen, por ejemplo, el nombre, orden y metadatos de las variables, además de compatibilizaciones cuando hay cambios en versiones (por
ejemplo, si una variable cambia de nombre de un año a otro).

!!! Info "Cada tabla del conjunto de datos debe tener su propia tabla de arquitectura (hoja de cálculo), que debe ser completada en **Google Drive** para permitir la corrección por nuestro equipo de datos."

#### Ejemplo: RAIS - Tablas de arquitectura

Las tablas de arquitectura de RAIS [pueden ser consultadas aquí](https://docs.google.com/spreadsheets/d/1dPLUCeE4MSjs0ykYUDsFd-e7-9Nk6LVV/edit?usp=sharing&ouid=103008455637924805982&rtpof=true&sd=true). Son una excelente referencia para que empieces tu trabajo ya que tienen muchas variables y ejemplos de diversas situaciones que puedes encontrar.

#### Para completar cada tabla de tu conjunto sigue este paso a paso:

!!! Tip "Al inicio y final de cada etapa consulta nuestro [manual de estilo](style_data.md) para garantizar que estás siguiendo la estandarización de BD"

1. Listar todas las variables de los datos en la columna `original_name`
    - Obs: Si la base cambia el nombre de las variables a lo largo de los años (como RAIS), es necesario hacer la compatibilización entre años para todas las variables completando la columna de `original_name_YYYY` para cada año o mes disponible
2. Renombrar las variables según nuestro [manual](style_data.md) en la columna `name`
3. Entender el tipo de variable y completar la columna `bigquery_type`
4. Completar la descripción en `description` según el [manual](style_data.md)
5. A partir de la compatibilización entre años y/o consultas a los datos brutos, completar la cobertura temporal en `temporal_coverage` de cada variable
    - Obs: Si las variables tienen la misma cobertura temporal que la tabla completar solo con '(1)'
6. Indicar con 'yes' o 'no' si hay diccionario para las variables en `covered_by_dictionary`
7. Verificar si las variables representan alguna entidad presente en los [directorios](https://basedosdados.org/dataset?q=diret%C3%B3rio&datasets_with=open_tables&organization=bd&page=1) para completar el `directory_column`
8. Para las variables del tipo `int64` o `float64` verificar si es necesario incluir una [unidad de medida](https://github.com/basedosdados/website/blob/master/ckanext-basedosdados/ckanext/basedosdados/validator/available_options/measurement_unit.py)
9. Reordenar las variables según el [manual](style_data.md)

!!! Tip "Cuando termines de completar las tablas de arquitectura, contacta con el equipo de Base de los Datos para validar todo. Es necesario que esté claro el formato final que los datos deben tener _antes_ de empezar a escribir el código. Así evitamos el retrabajo."

### 4. Escribir código de captura y limpieza de datos

Después de validadas las tablas de arquitectura, podemos escribir los códigos de
**captura** y **limpieza** de los datos.

- **Captura**: Código que descarga automáticamente todos los datos originales y los guarda en `/input`. Estos datos pueden estar disponibles en portales o enlaces FTP, pueden ser raspados de sitios, entre otros.

- **Limpieza**: Código que transforma los datos originales guardados en `/input` en datos limpios, guarda en la carpeta `/output`, para, posteriormente, ser subidos a BD.

Cada tabla limpia para producción puede ser guardada como un archivo único o, si es muy grande (por ejemplo, por encima de 200 mb), ser particionada en el formato [Hive](https://cloud.google.com/bigquery/docs/hive-partitioned-loads-gcs) en varios sub-archivos. Los formatos aceptados son `.csv` o `.parquet`. Nuestra recomendación es particionar tablas por `ano`, `mes` y `sigla_uf`. El particionamiento se hace a través de la estructura de carpetas, ve el ejemplo abajo para visualizar cómo.

#### Ejemplo: RAIS - Particionamiento
La tabla `microdados_vinculos` de RAIS, por ejemplo, es una tabla muy grande (+250GB) por eso nosotros particionamos por `ano` y `sigla_uf`. El particionamiento se hizo usando la estructura de carpetas `/microdados_vinculos/ano=YYYY/sigla_uf=XX` .

#### Estándares necesarios en el código

- Deben ser escritos en [Python](https://www.python.org/),
  [R](https://www.r-project.org/) o [Stata](https://www.stata.com/) -
  para que la revisión pueda ser realizada por el equipo.
- Puede estar en script (`.py`, `.R`, ...) o *notebooks* (Google Colab, Jupyter, Rmarkdown, etc).
- Las rutas de archivos deben ser atajos _relativos_ a la carpeta raíz
  (`<dataset_id>`), es decir, no deben depender de las rutas de tu
  computadora.
- La limpieza debe seguir nuestro [manual de estilo](style_data.md) y las [mejores prácticas de programación](https://en.wikipedia.org/wiki/Best_coding_practices).

#### Ejemplo: PNAD Continua - Código de limpieza

El código de limpieza fue construido en R y [puede ser consultado
aquí](https://github.com/basedosdados/mais/tree/master/bases/br_ibge_pnadc/code).

#### Ejemplo: Actividad en la Cámara Legislativa - Código de descarga y limpieza
El código de limpieza fue construido en Python [puede ser consultado aquí](https://github.com/basedosdados/mais/tree/bea9a323afcea8aa1609e9ade2502ca91f88054c/bases/br_camara_atividade_legislativa/code)

### 5. (Si es necesario) Organizar archivos auxiliares

Es común que las bases de datos sean disponibilizadas con archivos auxiliares. Estos pueden incluir notas técnicas, descripciones de recolección y muestreo, etc. Para ayudar a los usuarios de Base de los Datos a tener más contexto y entender mejor los datos, organiza todos estos archivos auxiliares en `/extra/auxiliary_files`.

Siéntete libre de estructurar sub-carpetas como quieras allí dentro. Lo que importa es que quede claro qué son estos archivos.

### 6. (Si es necesario) Crear tabla diccionario

Muchas veces, especialmente con bases antiguas, hay múltiples diccionarios en formatos Excel u otros. En Base de los Datos unificamos todo en un único archivo en formato `.csv` - un único diccionario para todas las columnas de todas las tablas de tu conjunto.

!!! Info "Detalles importantes de cómo construir tu diccionario están en nuestro [manual de estilo](../style_data/#dicionarios)."

#### Ejemplo: RAIS - Diccionario

El diccionario completo [puede ser consultado
aquí](https://docs.google.com/spreadsheets/d/12Wwp48ZJVux26rCotx43lzdWmVL54JinsNnLIV3jnyM/edit?usp=sharing).
Ya posee la estructura estándar que utilizamos para diccionarios.

### 7. Subir todo a Google Cloud

¡Todo listo! Ahora solo falta subir a Google Cloud y enviar para
revisión. Para esto, vamos a usar el cliente `basedosdados` (disponible en Python) que facilita las configuraciones y etapas del proceso.

!!! Info "Como existe un costo para el almacenamiento en storage, para finalizar esta etapa necesitaremos proporcionarte una api_key específica para voluntarios para subir los datos en nuestro ambiente de desarrollo. Así que únete a nuestro [canal en Discord](https://discord.gg/huKWpsVYx4) y llámanos en 'quiero-contribuir'"

#### Configura tus credenciales localmente
**7.1** En tu terminal instala nuestro cliente: `pip install basedosdados`.

**7.2** Ejecuta `import basedosdados as bd` en python y sigue el paso a paso para configurar localmente con las credenciales de tu proyecto en Google Cloud. Completa la información como sigue:
```
    * STEP 1: y
    * STEP 2: basedosdados-dev  (colocar el .json pasado por el equipo de bd en la carpeta credentials)
    * STEP 3: y
    * STEP 4: basedosdados-dev
    * STEP 5: https://api.basedosdados.org/api/v1/graphql
```
#### Sube los archivos a la Cloud
Los datos pasarán por 3 lugares en Google Cloud:

  * **Storage**: también llamado GCS es el lugar donde serán almacenados los archivos "fríos" (arquitecturas, datos, archivos auxiliares).
  * **BigQuery-DEV-Staging**: tabla que conecta los datos del storage al proyecto basedosdados-dev en bigquery
  * **BigQuery-DEV-Producción**: tabla utilizada para pruebas y tratamiento vía SQL del conjunto de datos

**7.3** Crea la tabla en el *bucket del GCS* y *BigQuey-DEV-staging*, usando la API de Python, de la siguiente forma:

    ```python
    import basedosdados as bd

    tb = bd.Table(
      dataset_id='<dataset_id>',
      table_id='<table_id>')

    tb.create(
        path='<ruta_para_los_datos>',
        if_table_exists='raise',
        if_storage_data_exists='raise',
    )
    ```

    Los siguientes parámetros pueden ser usados:


    - `path` (obligatorio): la ruta completa del archivo en tu computadora, como: `/Users/<tu_usuario>/proyectos/basedosdados/mais/bases/[DATASET_ID]/output/microdados.csv`.


    !!! Tip "Si tus datos están particionados, la ruta debe apuntar a la carpeta donde están las particiones. En caso contrario, debe apuntar a un archivo `.csv` (por ejemplo, microdados.csv)."

    - `force_dataset`: comando que crea los archivos de configuración del dataset en BigQuery.
        - _True_: los archivos de configuración del dataset serán creados en tu proyecto y, si no existe en BigQuery, será creado automáticamente. **Si ya has creado y configurado el dataset, no uses esta opción, pues sobrescribirá archivos**.
        - _False_: el dataset no será recreado y, si no existe, será creado automáticamente.
    - `if_table_exists` : comando utilizado si la **tabla ya existe en BQ**:
        - _raise_: retorna mensaje de error.
        - _replace_: sustituye la tabla.
        - _pass_: no hace nada.

    - `if_storage_data_exists`: comando utilizado si los **datos ya existen en Google Cloud Storage**:
        - _raise_: retorna mensaje de error
        - _replace_: sustituye los datos existentes.
        - _pass_: no hace nada.

    !!! Info "Si el proyecto no existe en BigQuery, será automáticamente creado"

  Consulta también nuestra [API](../api_reference_cli) para más detalles de cada método.

**7.4** Crea los archivos .sql y schema.yml a partir de la tabla de arquitectura siguiendo esta [documentación](https://github.com/basedosdados/pipelines/wiki/Fun%C3%A7%C3%A3o-%60create_yaml_file()%60)
!!! Tip "Si lo necesitas, en este momento puedes alterar la consulta en SQL para realizar tratamientos finales a partir de la tabla `staging`, puedes incluir columna, remover columna, hacer operaciones algebraicas, sustituir strings, etc. ¡El SQL es el límite!"

**7.5** Ejecuta y prueba los modelos localmente siguiendo esta [documentación](https://github.com/basedosdados/pipelines/wiki/Testar-modelos-dbt-localmente)

**7.6** Sube los metadatos de la tabla en el sitio:
!!! Info "Por ahora solo el equipo de datos tiene permisos para subir los metadatos de la tabla en el sitio, por eso será necesario contactar con nosotros. Ya estamos trabajando para que, en un futuro próximo, los voluntarios también puedan actualizar datos en el sitio."

**7.7** Sube los archivos auxiliares:
    ```python
    st = bd.Storage(
      dataset_id = <dataset_id>,
      table_id = <table_id>)

    st.upload(
      path='ruta_para_los_archivos_auxiliares',
      mode = 'auxiliary_files',
      if_exists = 'raise')
    ```

### 8. Enviar todo para revisión

¡Uf, eso es todo! Ahora solo queda enviar todo para revisión en el
[repositorio](https://github.com/basedosdados/queries-basedosdados) de Base de los Datos.

1. Clona nuestro [repositorio](https://github.com/basedosdados/queries-basedosdados) localmente.
2. Da un `cd` a la carpeta local del repositorio y abre una nueva branch con `git checkout -b [dataset_id]`. Todas las adiciones y modificaciones serán incluidas en esa _branch_.
3. Para cada tabla nueva incluir el archivo con nombre `table_id.sql` en la carpeta `queries-basedosdados/models/dataset_id/` copiando las queries que desarrollaste en el paso 7.
4. Incluir el archivo schema.yaml desarrollado en el paso 7
5. Si es un dataset nuevo, incluir el dataset conforme las instrucciones del archivo `queries-basedosdados/dbt_project.yaml` (no te olvides de seguir el orden alfabético para no desordenar la organización)
6. Incluye tu código de captura y limpieza en la carpeta `queries-basedosdados/models/dataset_id/code`
7. Ahora solo falta publicar la branch, abrir el PR con las labels 'table-approve' y marcar al equipo de datos para corrección

**¿Y ahora?** Nuestro equipo revisará los datos y metadatos enviados
vía Github. Podemos contactarte para resolver dudas o solicitar
cambios en el código. Cuando todo esté OK, hacemos un _merge_ de tu
*pull request* y los datos son automáticamente publicados en nuestra
plataforma!
