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

  if (is.null(search) || is.null(search$datasets) || length(search$datasets) == 0) {
    return(tibble::tibble(
      dataset_name = character(0),
      dataset_tables = list(),
      url = character(0),
      title = character(0)
    ))
  }

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
      purrr::reduce(dplyr::bind_rows, .init = tibble::tibble(name = character(0), description = character(0)))

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

  result <- basedosdados::bd_request(
    endpoint = "bdm_table_show",
    query = list(
      table_id = table_id,
      dataset_id = dataset_id))

  if (is.null(result) || is.null(result$columns) || length(result$columns) == 0) {
    return(tibble::tibble(
      name = character(0),
      description = character(0)
    ))
  }

  result %>%
    purrr::pluck("columns") %>%
    purrr::map(tibble::as_tibble) %>%
    purrr::reduce(dplyr::bind_rows, .init = tibble::tibble()) %>%
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

  result <- basedosdados::bd_request(
    endpoint = "bdm_dataset_show",
    query = list(
      dataset_id = dataset_id))

  if (is.null(result)) {
    return(tibble::tibble(
      name = NA_character_,
      title = NA_character_,
      tables = list(tibble::tibble(name = character(0), description = character(0))),
      notes = NA_character_
    ))
  }

  tibble::tibble(
    name = if (is.null(result$name)) NA_character_ else result$name,
    title = if (is.null(result$title)) NA_character_ else result$title,
    tables = list(list_dataset_tables(dataset_id)),
    notes = if (is.null(result$notes)) NA_character_ else result$notes)

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

  result <- basedosdados::bd_request(
    endpoint = "bdm_table_show",
    query = list(
      dataset_id = dataset_id,
      table_id = table_id))

  if (is.null(result)) {
    return(tibble::tibble(
      dataset_id = dataset_id,
      table_id = table_id,
      description = NA_character_,
      columns = list(tibble::tibble())
    ))
  }

  columns_data <- if (is.null(result$columns) || length(result$columns) == 0) {
    tibble::tibble()
  } else {
    result %>%
      purrr::pluck("columns") %>%
      purrr::map(tibble::as_tibble) %>%
      purrr::reduce(dplyr::bind_rows, .init = tibble::tibble())
  }

  tibble::tibble(
    dataset_id = dataset_id,
    table_id = table_id,
    description = if (is.null(result$description)) NA_character_ else result$description,
    columns = list(columns_data))

}
