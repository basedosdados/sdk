# Python

Esta API está compuesta por funciones con 2 tipos de funcionalidad:

- Módulos para **solicitud de datos**: para aquellos que desean
  solamente consultar los datos y metadatos de nuestro proyecto.

- Clases para **gestión de datos** en Google Cloud: para
  aquellos que desean subir datos en nuestro proyecto (o cualquier otro
  proyecto en Google Cloud, siguiendo nuestra metodología e infraestructura).

!!! Info "Toda la documentación del código siguiente está en inglés"

## Módulos (Solicitud de datos)

::: basedosdados.download.metadata
    handler: python
    rendering:
            show_root_heading: no
            heading_level: 3
    selection:
      docstring_style: google  # this is the default
      docstring_options:
        replace_admonitions: no

::: basedosdados.download.download
    handler: python
    rendering:
            show_root_heading: no
            heading_level: 3
    selection:
      docstring_style: google  # this is the default
      docstring_options:
        replace_admonitions: no

## Clases (Gestión de datos)

::: basedosdados.upload.storage
    handler: python
    rendering:
            show_root_heading: no
            heading_level: 3
    selection:
      docstring_style: google  # this is the default
      docstring_options:
        replace_admonitions: no

---
::: basedosdados.upload.dataset
    handler: python
    rendering:
            show_root_heading: no
            heading_level: 3
    selection:
      docstring_style: google  # this is the default
      docstring_options:
        replace_admonitions: no

---
::: basedosdados.upload.table
    handler: python
    rendering:
            show_root_heading: no
            heading_level: 3
    selection:
      docstring_style: google  # this is the default
      docstring_options:
        replace_admonitions: no