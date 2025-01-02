# Manual de estilo

En esta sección listamos todos los estándares de nuestro manual de estilo y directrices de datos que usamos en Base de los Datos. Estos nos ayudan a mantener los datos y metadatos que publicamos con alta calidad.

!!! Tip "Puedes usar el menú izquierdo para navegar por los diferentes temas de esta página."

---

## Nomenclatura de bases y tablas

### Conjuntos de datos (`dataset_id`)

Nombramos conjuntos en el formato `<organization_id>_<descripción>`, donde
`organization_id` sigue por defecto la **cobertura geográfica de la
organización** que publica el conjunto:

|           | organization_id                         |
|-----------|----------------------------------------------|
| Mundial   | `mundo_<organizacion>`                         |
| Federal   | `<sigla_pais>_<organizacion>`                         |
| Estatal   | `<sigla_pais>_<sigla_estado>_<organizacion>`             |
| Municipal | `<sigla_pais>_<sigla_estado>_<ciudad>_<organizacion>`   |

* `sigla_pais` y `sigla_estado` son siempre 2 letras minúsculas;
* `organizacion` es el nombre o sigla (preferentemente) de la organización que
  publicó los datos originales (ej: `ibge`, `tse`, `inep`).
* `descripcion` es una breve descripción del conjunto de datos

Por ejemplo, el conjunto de datos del PIB del IBGE tiene como `dataset_id`: `br_ibge_pib`

!!! Tip "¿No sabes cómo nombrar la organización?"
    Sugerimos que vayas al sitio web de la misma y veas cómo se autodenomina (ej: DETRAN-RJ sería `br_rj_detran`)

### Tablas

Nombrar tablas es algo menos estructurado y, por eso, requiere sentido común. Pero tenemos algunas reglas:

- Si hay tablas para diferentes entidades, incluir la entidad al principio del nombre. Ejemplo: `municipio_valor`, `uf_valor`.
- No incluir la unidad temporal en el nombre. Ejemplo: nombrar `municipio`, y no `municipio_ano`.
- Dejar nombres en singular. Ejemplo: `escuela`, y no `escuelas`.
- Nombrar como `microdatos` las tablas más desagregadas. En general estas tienen datos a nivel de persona o transacción.

### Ejemplos de `dataset_id.table_id`

|           |                                           |                                                     |
|-----------|-------------------------------------------|-----------------------------------------------------|
| Mundial   | `mundo_waze.alertas`                      | Datos de alertas de Waze de diferentes ciudades.    |
| Federal   | `br_tse_eleicoes.candidatos`              | Datos de candidatos a cargos políticos del TSE.      |
| Federal   | `br_ibge_pnad.microdados`                 | Microdatos de la Encuesta Nacional por Muestra de Hogares producidos por el IBGE. |
| Federal   | `br_ibge_pnadc.microdados`                | Microdatos de la Encuesta Nacional por Muestra de Hogares Continua (PNAD-C) producidos por el IBGE. |
| Estatal   | `br_sp_see_docentes.carga_horaria`        | Carga horaria anonimizada de docentes activos de la red estatal de enseñanza de SP. |
| Municipal | `br_rj_riodejaneiro_cmrj_legislativo.votaciones` | Datos de votación de la Cámara Municipal de Río de Janeiro (RJ). |

## Formatos de tablas

Las tablas deben, en la medida de lo posible, estar en formato `long`, en lugar de `wide`.

## Nomenclatura de variables

Los nombres de variables deben respetar algunas reglas:

- Usar al máximo nombres ya presentes en el repositorio. Ejemplos: `ano`, `mes`, `id_municipio`, `sigla_uf`, `edad`, `cargo`, `resultado`, `votos`, `ingreso`, `gasto`, `precio`, etc.
- Respetar patrones de las tablas de directorios.
- Ser lo más intuitivo, claro y extenso posible.
- Tener todas las letras minúsculas (inclusive siglas), sin acentos, conectados por `_`.
- No incluir conectores como `de`, `la`, `los`, `y`, `en`, etc.
- Solo tener el prefijo `id_` cuando la variable represente claves primarias de entidades (que eventualmente tendrían una tabla de directorio).
    - Ejemplos que tienen: `id_municipio`, `id_uf`, `id_escuela`, `id_persona`.
    - Ejemplos que no tienen: `red`, `localizacion`.
    - **Importante**: cuando la base está en inglés id se convierte en un sufijo
- Solo tener sufijos de entidad cuando la entidad de la columna sea diferente de la entidad de la tabla.
    - Ejemplos que tienen: en una tabla con entidad `persona`, una columna sobre PIB municipal se llamaría `pib_municipio`.
    - Ejemplos que no tienen: en una tabla con entidad `persona`, características de la persona se llamarían `nombre`, `edad`, `sexo`, etc.
- Lista de **prefijos permitidos**
    - `nombre_`,
    - `fecha_`,
    - `numero_`,
    - `cantidad_`,
    - `proporcion_` (variables de porcentaje 0-100%),
    - `tasa_`,
    - `razon_`,
    - `indice_`,
    - `indicador_` (variables de tipo booleano),
    - `tipo_`,
    - `sigla_`,
    - `secuencial_`.
- Lista de **sufijos comunes**
    - `_pc` (per cápita)

## Ordenamiento de variables

El orden de variables en tablas está estandarizado para mantener una consistencia en el repositorio. Nuestras reglas son:

- Claves primarias a la izquierda, en orden descendente de cobertura;
- En el medio deben estar variables cualitativas de la línea;
- Las últimas variables deben ser los valores cuantitativos en orden creciente de relevancia;
- Ejemplo de orden: `ano`, `sigla_uf`, `id_municipio`, `id_escuela`, `red`, `nota_ideb`;
- Dependiendo de la tabla, puede ser recomendado agrupar y ordenar variables por temas.

## Tipos de variables

Utilizamos algunas de las opciones de [tipos de BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types): `string`, `int64`, `float64`, `date`, `time`, `geography`.

Cuándo elegir:

- `string`:
    - Variables de texto
    - Claves de variables categóricas con diccionario o directorio
- `int64`:
    - Variables de números enteros con las que es posible hacer cálculos (adición, sustracción)
    - Variables de tipo booleanas que llenamos con 0 o 1
- `float64`:
    - Variables de números con decimales con las que es posible hacer cálculos (adición, sustracción)
- `date`:
    - Variables de fecha en formato `YYYY-MM-DD`
- `time`:
    - Variables de tiempo en formato `HH:MM:SS`
- `geography`:
    - Variables de geografía

## Unidades de medida

La regla es mantener variables con sus unidades de medida originales listadas en este [código](https://github.com/basedosdados/website/blob/master/ckanext-basedosdados/ckanext/basedosdados/validator/available_options/measurement_unit.py), con la excepción de variables financieras donde convertimos monedas antiguas a las actuales (ej. Cruzeiro a Real).

Catalogamos unidades de medida en formato estándar en la tabla de arquitectura. [Lista completa aquí](https://github.com/basedosdados/website/blob/master/ckanext-basedosdados/ckanext/basedosdados/validator/available_options/measurement_unit.py) Ejemplos: `m`, `km/h`, `BRL`.

Para columnas financieras deflactadas, listamos la moneda con el año base. Ejemplo: una columna medida en reales de 2010 tiene unidad `BRL_2010`.

Las variables deben tener siempre unidades de medida con base 1. Es decir, tener `BRL` en lugar de `1000 BRL`, o `persona` en lugar de `1000 personas`. Esta información, como otros metadatos de columnas, se registra en la tabla de arquitectura de la tabla.

## Qué variables mantener, cuáles añadir y cuáles eliminar

Mantenemos nuestras tablas parcialmente [normalizadas](https://www.guru99.com/database-normalization.html), y tenemos reglas para qué variables incluir en producción. Estas son:

- Eliminar variables de nombres de entidades que ya están en directorios. Ejemplo: retirar `municipio` de la tabla que ya incluye `id_municipio`.
- Eliminar variables que sirven de partición. Ejemplo: eliminar `ano` y `sigla_uf` si la tabla está particionada en estas dos dimensiones.
- Añadir claves primarias principales para cada entidad ya existente. Ejemplo: añadir `id_municipio` a tablas que solo incluyen `id_municipio_tse`.
- Mantener todas las claves primarias que ya vienen con la tabla, pero (1) añadir claves relevantes (ej. `sigla_uf`, `id_municipio`) y (2) retirar claves irrelevantes (ej. `region`).

## Cobertura temporal

Llenar la columna `cobertura_temporal` en los metadatos de tabla, columna y clave (en diccionarios) sigue el siguiente patrón.

- Formato general: `fecha_inicial(unidad_temporal)fecha_final`
    - `fecha_inicial` y `fecha_final` están en la correspondiente unidad temporal.
        - Ejemplo: tabla con unidad `ano` tiene cobertura `2005(1)2018`.
        - Ejemplo: tabla con unidad `mes` tiene cobertura `2005-08(1)2018-12`.
        - Ejemplo: tabla con unidad `semana` tiene cobertura `2005-08-01(7)2018-08-31`.
        - Ejemplo: tabla con unidad `dia` tiene cobertura `2005-08-01(1)2018-12-31`.

- Reglas para llenado
    - Metadatos de tabla
        - Llenar en el formato general.
    - Metadatos de columna
        - Llenar en el formato general, excepto cuando la `fecha_inicial` o `fecha_final` sean iguales a las de la tabla. En ese caso dejar vacío.
        - Ejemplo: suponga que la cobertura de la tabla sea `2005(1)2018`.
            - Si una columna aparece solo en 2012 y existe hasta 2018, llenamos su cobertura como `2012(1)`.
            - Si una columna desaparece en 2013, llenamos su cobertura como `(1)2013`.
            - Si una columna existe en la misma cobertura temporal que la tabla, llenamos su cobertura como `(1)`.
    - Metadatos de clave
        - Llenar en el mismo patrón de columnas, pero la referencia siendo la columna correspondiente, y no la tabla.

## Limpiando STRINGs

- Variables categóricas: inicial mayúscula y resto minúscula, con acentos.
- STRINGs no estructuradas: mantener igual a los datos originales.

## Formatos de valores

- Decimal: formato americano, es decir siempre `.` (punto) en lugar de `,` (coma).
- Fecha: `YYYY-MM-DD`
- Horario (24h): `HH:MM:SS`
- Datetime ([ISO-8601](https://en.wikipedia.org/wiki/ISO_8601)): `YYYY-MM-DDTHH:MM:SS.sssZ`
- Valor nulo: `""` (csv), `NULL` (Python), `NA` (R), `.` o `""` (Stata)
- Proporción/porcentaje: entre 0-100

## Particionamiento de tablas

### **¿Qué es el particionamiento y cuál es su objetivo?**

De forma resumida, particionar una tabla es dividirla en varios bloques/partes. El objetivo central es disminuir los costos financieros y aumentar el rendimiento, ya que, cuanto mayor sea el volumen de datos, consecuentemente será mayor el costo de almacenamiento y consulta.

La reducción de costos y el aumento de rendimiento ocurre, principalmente, porque la partición permite la reorganización del conjunto de datos en pequeños **bloques agrupados**. En la práctica, realizando el particionamiento, es posible evitar que una consulta recorra toda la tabla solo para traer un pequeño recorte de datos.

Un ejemplo práctico de nuestra querida RAIS:

- Sin utilizar filtro de partición:

Para este caso, BigQuery recorrió todas (*) las columnas y filas del conjunto. Vale señalar que este costo aún no es tan grande, ya que la base ya fue particionada. Si este conjunto no hubiera pasado por el proceso de particionamiento, esta consulta costaría mucho más dinero y tiempo, ya que se trata de un volumen considerable de datos.

![image](https://user-images.githubusercontent.com/58278652/185815101-68ed5797-fff8-4968-84e2-e6a47bba58d0.png)

- Con filtro de partición:

Aquí, filtramos por las columnas particionadas `ano` y `sigla_uf`. De esta forma, BigQuery solo **consulta** y **retorna** los valores de la carpeta **ano** y la subcarpeta **sigla_uf**.

![image](https://user-images.githubusercontent.com/58278652/185815135-fb012a2c-535b-457e-af2a-7984961168b3.png)

### **¿Cuándo particionar una tabla?**

La primera pregunta que surge cuando se trata de particionamiento es: _¿a partir de qué cantidad de filas una tabla debe ser particionada?_ La documentación de [GCP](https://cloud.google.com/bigquery/docs/partitioned-tables?hl=es) no define una cantidad _x_ o _y_ de filas que debe ser particionada. Lo ideal es que las tablas sean particionadas, con pocas excepciones. Por ejemplo, tablas con menos de 10.000 filas, que no recibirán más ingestión de datos, no tienen un costo de almacenamiento y procesamiento altos y, por lo tanto, no hay necesidad de ser particionadas.

### **¿Cómo particionar una tabla?**

Si los datos están guardados localmente, es necesario:

1. Crear las carpetas particionadas en tu carpeta de `/output`, en el lenguaje que estés utilizando.

Ejemplo de una tabla particionada por `ano` y `mes`, utilizando `python`:

```python
for ano in [*range(2005, 2020)]:
  for mes in [*range(1, 13)]:
    particion = output + f'table_id/ano={ano}/mes={mes}'
    if not os.path.exists(particion):
      os.makedirs(particion)
```
2. Guardar los archivos particionados.

```python
for ano in [*range(2005, 2020)]:
  for mes in [*range(1, 13)]:
    df_particion = df[df['ano'] == ano].copy() # El .copy no es necesario, es solo una buena práctica
    df_particion = df_particion[df_particion['mes'] == mes]
    df_particion.drop(['ano', 'mes'], axis=1, inplace=True) # Es necesario excluir las columnas utilizadas para partición
    particion = output + f'table_id/ano={ano}/mes={mes}/tabla.csv'
    df_particion.to_csv(particion, index=False, encoding='utf-8', na_rep='')
```

Ejemplos de tablas particionadas en `R`:

- [PNADC](https://github.com/basedosdados/mais/blob/master/bases/br_ibge_pnadc/code/microdados.R)
- [PAM](https://github.com/basedosdados/mais/blob/master/bases/br_ibge_pam/code/permanentes_usando_api.R)

Ejemplo de cómo particionar una tabla en `SQL`:

```sql
CREATE TABLE `dataset_id.table_id` as (
    ano  INT64,
    mes  INT64,
    col1 STRING,
    col1 STRING
) PARTITION BY ano, mes
OPTIONS (Description='Descripción de la tabla')
```

### **Reglas importantes de particionamiento**

- Los tipos de columnas que BigQuery acepta como partición son:

  * **Columna de unidad de tiempo**: las tablas son particionadas con base en una columna de `TIMESTAMP`, `DATE` o `DATETIME`.
  * **Tiempo de procesamiento**: las tablas son particionadas con base en el sello de `fecha/hora` cuando BigQuery procesa los datos.
  * **Intervalo de números enteros**: las tablas son particionadas con base en una columna de números enteros.

- Los tipos de columnas que BigQuery **no** acepta como partición son: `BOOL`, `FLOAT64`, `BYTES`, etc.

- BigQuery acepta como máximo 4.000 particiones por tabla.

- Aquí en BD las tablas generalmente son particionadas por: `ano`, `mes`, `trimestre` y `sigla_uf`.

- Note que al particionar una tabla es necesario excluir la columna correspondiente. Ejemplo: es necesario excluir la columna `ano` al particionar por `ano`.

## Número de bases por _pull request_

Los pull requests en Github deben incluir como máximo un conjunto, pero pueden incluir más de una base. Es decir, pueden involucrar una o más tablas dentro del mismo conjunto.

## Diccionarios

- Cada base incluye solamente un diccionario (que cubre una o más tablas).
- Para cada tabla, columna, y cobertura temporal, cada clave mapea únicamente un valor.
- Las claves no pueden tener valores nulos.
- Los diccionarios deben cubrir todas las claves disponibles en las tablas originales.
- Las claves solo pueden poseer ceros a la izquierda cuando el número de dígitos de la variable tenga significado. Cuando la variable sea `enum` estándar, excluimos los ceros a la izquierda.
    - Ejemplo: mantenemos el cero a la izquierda de la variable `br_bd_diretorios_brasil.cbo_2002:cbo_2002`, que tiene seis dígitos, pues el primer dígito `0` significa que la categoría es del `gran grupo = "Miembros de las fuerzas armadas, policías y bomberos militares"`.
    - Para otros casos, como por ejemplo `br_inep_censo_escolar.turma:etapa_ensino`, excluimos los ceros a la izquierda. Es decir, cambiamos `01` por `1`.
- Los valores son estandarizados: sin espacios extras, inicial mayúscula y resto minúscula, etc.

### **¿Cómo llenar los metadatos de la tabla diccionario?**
- No llenar el *`spatial_coverage`* (`cobertura_espacial`), es decir, dejar el campo vacío.
- No llenar el *`temporal_coverage`* (`cobertura_temporal`), es decir, dejar el campo vacío.
- No llenar el *`observation_level`* (`nivel_observacion`), es decir, dejar el campo vacío.

## Directorios

Los directorios son las piedras fundamentales de la estructura de nuestro _data lake_. Nuestras reglas para gestionar directorios son:

- Los directorios representan _entidades_ del repositorio que tengan claves primarias (ej. `uf`, `municipio`, `escuela`) y unidades de fecha-tiempo (ej. `fecha`, `tiempo`, `dia`, `mes`, `ano`).
- Cada tabla de directorio tiene al menos una clave primaria con valores únicos y sin nulos. Ejemplos: `municipio:id_municipio`, `uf:sigla_uf`.
- Los nombres de variables con prefijo `id_` están reservados para claves
  primarias de entidades.

Vea todas las [tablas ya disponibles aquí.](https://basedosdados.org/dataset?organization=br-bd&order_by=score&q=%22directorios%22)

### **¿Cómo llenar los metadatos de las tablas de directorio?**
- Llenar el *`spatial_coverage`* (`cobertura_espacial`), que es la máxima unidad espacial que la tabla cubre. Ejemplo: sa.br, que significa que el nivel de agregación espacial de la tabla es Brasil.
- No llenar el *`temporal_coverage`* (`cobertura_temporal`), es decir, dejar el campo vacío.
- Llenar el *`observation_level`* (`nivel_observacion`), que consiste en el nivel de observación de la tabla, es decir, lo que representa cada fila.
- No llenar el *`temporal_coverage`* (`cobertura_temporal`) de las columnas de la tabla, es decir, dejar el campo vacío.

## Fuentes Originales

El campo se refiere a los datos en la fuente original, que aún no han pasado por la metodología de tratamiento de Base de los Datos, es decir, nuestro `_input_`. Al hacer clic en él, la idea es redirigir al usuario a la página de la fuente original de los datos. Las reglas para gestionar las Fuentes Originales son:

- Incluir el nombre del enlace externo que lleva a la fuente original. Como estándar, este nombre debe ser de la organización o del portal que almacena los datos. Ejemplos: `Sinopsis Estadísticas de la Educación Básica: Datos Abiertos del Inep`, `Penn World Tables: Groningen Growth and Development Centre`.
- Llenar los metadatos de Fuentes Originales: Descripción, URL, Idioma, Tiene Datos Estructurados, Tiene una API, Es Gratuito, Requiere Registro, Disponibilidad, Requiere IP de Algún País, Tipo de Licencia, Cobertura Temporal, Cobertura Espacial y Nivel de Observación.

## **¿Pensaste en mejoras para los estándares definidos?**

Abre un [issue en nuestro Github](https://github.com/basedosdados/mais/labels/docs) o envía un mensaje en [Discord](https://discord.gg/huKWpsVYx4) para conversar :)
