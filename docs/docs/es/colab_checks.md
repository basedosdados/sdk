# Colaborando con pruebas en BD

Para mantener la calidad de las bases de datos presentes en BD, contamos con un
conjunto de verificaciones automáticas que se realizan durante la inserción y
actualización de cada base. Estas verificaciones son necesarias, pero no suficientes
para garantizar la calidad de los datos. Realizan consultas básicas, como si la
tabla existe o si tiene columnas totalmente nulas.

Puedes colaborar con BD aumentando la cobertura de las pruebas, disminuyendo así
el trabajo de revisión de los datos. Para esto basta con crear consultas que prueben la
calidad de los datos en SQL, como las siguientes:

- Verificar si las columnas con proporción tienen valores entre 0 y 100
- Verificar si las columnas con fechas siguen el patrón YYYY-MM-DD HH:MM:SS

<!----------------------------------------------------------------------------->

## ¿Cuál es el procedimiento?

Incluir pruebas de datos debe seguir el flujo de trabajo:

- [Colaborando con pruebas en BD](#colaborando-con-pruebas-en-bd)
  - [¿Cuál es el procedimiento?](#cuál-es-el-procedimiento)
  - [1. Informa tu interés](#1-informa-tu-interés)
  - [2. Escribe tu consulta](#2-escribe-tu-consulta)
  - [3. Envía tu consulta](#3-envía-tu-consulta)

!!! Tip "¡Sugerimos que te unas a nuestro [canal de Discord](https://discord.gg/huKWpsVYx4) para resolver dudas e interactuar con otros(as) colaboradores(as)! :)"

<!----------------------------------------------------------------------------->

## 1. Informa tu interés

Conversa con nosotros en el chat de infraestructura en Discord. Si no tienes una sugerencia de mejora, podemos buscar alguna consulta que aún no haya sido escrita.

<!----------------------------------------------------------------------------->

## 2. Escribe tu consulta

Haz un fork del repositorio de [Base de los Datos](https://github.com/basedosdados/mais/tree/master).
Luego agrega nuevas consultas y sus respectivas funciones de ejecución en los archivos
[checks.yaml](https://github.com/basedosdados/mais/blob/master/.github/workflows/data-check/checks.yaml)
y [test_data.py](https://github.com/basedosdados/mais/blob/master/.github/workflows/data-check/test_data.py).

Las consultas se escriben en un archivo YAML con `Jinja` y SQL, de la siguiente forma:

```yaml
test_select_all_works:
  name: Check if select query in {{ table_id }} works
  query: |
    SELECT NOT EXISTS (
            SELECT *
        FROM `{{ project_id_staging }}.{{ dataset_id }}.{{ table_id }}`
    ) AS failure
```

Y se ejecutan como pruebas del paquete `pytest`:

```python
def test_select_all_works(configs):
    result = fetch_data("test_select_all_works", configs)
    assert result.failure.values == False
```

No te asustes si no conoces algo de la sintaxis anterior, podemos ayudarte durante
el proceso. Ten en cuenta que los valores entre llaves son variables contenidas en archivos
`table_config.yaml`, que contienen metadatos de las tablas. Por lo tanto, la escritura de consultas
está limitada por los metadatos existentes. Recomendamos consultar estos archivos
en el directorio de las [bases](https://github.com/basedosdados/mais/tree/master/bases).

<!----------------------------------------------------------------------------->

## 3. Envía tu consulta

Finalmente, realiza un pull request al repositorio principal para que se realice una revisión de la consulta.
