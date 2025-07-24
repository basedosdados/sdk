#' Search for a dataset by keyword
#' @param search_term keyword for search
#'
#' @return A tibble with search results
#'
#' @importFrom purrr map_chr pluck
#' @importFrom stringr str_replace_all
#' @importFrom rlang .data
#'
#' @export
#' @examples
#'
#' \dontrun{
#'
#' dataset_search("agua")
#' dataset_search("educação")
#'
#'}
#'
#'


dataset_search <- function(search_term) {

  bd_request(
    endpoint = "dataset_search",
    query = list(
      resource_type = "bdm_table",
      q = search_term,
      page_size = 100)) ->
    search

  tibble::tibble(
    dataset_name = purrr::map_chr(
      .x = search$datasets,
      .f = ~ purrr::pluck(.x, "name") %>%
        stringr::str_replace_all("-", "_")),
    dataset_tables = purrr::map(
      .x = .data$dataset_name,
      .f = basedosdados::list_dataset_tables),
    url = purrr::map_chr(
      .x = search$datasets,
      .f = ~ glue::glue("https://basedosdados.org/dataset/{purrr::pluck(.x, 'id')}")),
    title = purrr::map_chr(
      .x = search$datasets,
      .f = ~ purrr::pluck(.x, "title")))

  }

#' List tables in a dataset
#' @param dataset_id a dataset name e.g. if addressing table "br_sp_alesp.deputado" then table_id is `br_sp_alesp`
#' @export
#' @importFrom purrr pluck map_chr discard
#' @importFrom dplyr bind_rows
#' @return A tibble listing all tables in a given dataset
#' @examples
#' \dontrun{
#' list_dataset_tables("br_sp_alesp")
#' }

list_dataset_tables <- function(dataset_id) {

    basedosdados::bd_request(
      endpoint = "bdm_dataset_show",
      query = list(
        dataset_id = dataset_id)) ->
    results

    fetch_function <- purrr::possibly(
      .f = function(resource) {

        tibble::tibble(
          name = ifelse(rlang::is_null(resource$name), NA_character_, resource$name),
          description = ifelse(rlang::is_null(resource$description), NA_character_, resource$description))

        },
      otherwise = "Error")

    results %>%
      purrr::pluck("resources") %>%
      purrr::keep(~ .x$resource_type == "bdm_table") %>%
      purrr::map(fetch_function) %>%
      purrr::reduce(dplyr::bind_rows)

  }

#' Get columns in a table
#' @param dataset_id a dataset name e.g. if addressing table "br_sp_alesp.deputado" then table_id is `br_sp_alesp`
#' @param table_id a table name e.g. if addressing table "br_sp_alesp.deputado" then table_id is `deputado`
#'
#' @export
#' @examples
#' \dontrun{
#' get_table_columns("br_sp_alesp", "deputado")
#' }
#' @importFrom httr content
#' @importFrom purrr pluck map reduce
#' @importFrom dplyr bind_rows
#' @return A tibble describing all columns in a table

get_table_columns <- function(
  dataset_id,
  table_id) {

  basedosdados::bd_request(
    endpoint = "bdm_table_show",
    query = list(
      table_id = table_id,
      dataset_id = dataset_id)) %>%
    purrr::pluck("columns") %>%
    purrr::map(tibble::as_tibble) %>%
    purrr::reduce(dplyr::bind_rows) %>%
    dplyr::select(- c(.data$is_in_staging, .data$is_partition))

  }


#' Describe a dataset
#' @param dataset_id a dataset name e.g. if addressing table "br_sp_alesp.deputado" then table_id is `br_sp_alesp`
#'
#' @export
#' @examples
#'
#' \dontrun{
#'
#' get_dataset_description("br_sp_alesp")
#' }
#' @return A tibble describing the specified dataset

get_dataset_description <- function(dataset_id) {

  basedosdados::bd_request(
    endpoint = "bdm_dataset_show",
    query = list(
      dataset_id = dataset_id)) ->
    result

  tibble::tibble(
    name = result$name,
    title = result$title,
    tables = list(list_dataset_tables(dataset_id)),
    notes = result$notes)

}

#' Describe a table within a dataset
#'
#' @param dataset_id a dataset name e.g. if addressing table "br_sp_alesp.deputado" then table_id is `br_sp_alesp`
#' @param table_id a table name e.g. if addressing table "br_sp_alesp.deputado" then table_id is `deputado`
#' @export
#' @examples
#' \dontrun{
#' get_table_description("br_sp_alesp", "deputado")
#' }
#' @return A tibble describing the specified table
#'

get_table_description <- function(
  dataset_id,
  table_id) {

  basedosdados::bd_request(
    endpoint = "bdm_table_show",
    query = list(
      dataset_id = dataset_id,
      table_id = table_id)) ->
    result

  tibble::tibble(
    dataset_id = dataset_id,
    table_id = table_id,
    description = result$description,
    columns = result %>%
      purrr::pluck("columns") %>%
      purrr::map(tibble::as_tibble) %>%
      purrr::reduce(dplyr::bind_rows) %>%
      list())

}
