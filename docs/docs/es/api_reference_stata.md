# Stata

Esta API está compuesta por módulos para **solicitud de datos**: para aquellos que desean
  solamente consultar los datos y metadatos de nuestro proyecto (o cualquier otro
  proyecto en Google Cloud).

!!! Info "Toda la documentación del código siguiente está en inglés"

## Módulos (Solicitud de datos)

Si es tu primera vez utilizando el paquete, escribe ```db basedosdados``` y confirma nuevamente si los pasos anteriores fueron completados con éxito.

El paquete contiene 7 comandos, con sus funcionalidades descritas a continuación:

| __Comando__               | __Descripción__                                                                    |
|---------------------------|-----------------------------------------------------------------------------------|
| `bd_download`             | descarga datos de Base de los Datos (BD).                                        |
| `bd_read_sql`             | descarga tablas usando consultas específicas.                              |
| `bd_read_table`           | descarga tablas usando `dataset_id` y `table_id`.                          |
| `bd_list_datasets`        | lista el `dataset_id` de los conjuntos de datos disponibles en `query_project_id`.|
| `bd_list_dataset_tables`  | lista `table_id` para tablas disponibles en el `dataset_id` especificado.         |
| `bd_get_table_description`| muestra la descripción completa de la tabla.                                  |
| `bd_get_table_columns`    | muestra los nombres, tipos y descripciones de las columnas en la tabla especificada.|

Cada comando tiene un __help file__ de apoyo, basta con abrir el help y seguir las instrucciones:

```
help [comando]
```
