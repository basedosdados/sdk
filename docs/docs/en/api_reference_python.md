
# Python

This API is composed of functions with two types of functionality:

- Modules for **data request**: for those who only want to consult the data and metadata of our project.

- Classes for **data management** in Google Cloud: for those who want to upload data to our project (or any other project in Google Cloud, following our methodology and infrastructure).

## Modules (Data requests)

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

## Classes (Data management)

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