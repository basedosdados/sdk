#' Compatibility with {dplyr} verbs without using SQL language
#'
#' @description
#'
#' Allow you to explore and perform operation with Base dos Dados' datasets
#' without using SQL language. The [bdplyr()] function creates `lazy` variables
#' that will be connected directly to the desired table from Base dos Dados at
#' Google BigQuery and can be handled with the [dplyr::dplyr-package]'s verbs
#' as traditionally done as local bases. See also: [bigrquery::src_bigquery].
#'
#' Therefore, it is possible (without using `SQL`) to perform, for example,
#' column selection with [dplyr::select()], filter rows with [dplyr::filter()],
#' operations with [dplyr::mutate()], joins with [dplyr::left_join()] and
#' other vebs from `{dplyr}` package.
#'
#' The data will be automatically be downloaded from Google BigQuery in the
#' background as it if necessary, but wille not be loaded into your virtual
#' memory nor recorded on disk unless expressly requested.
#'
#' For this, the functions such as [bd_collect()] or [bd_write()] should be
#' used. To load the data handled locally in your virtual memory, use
#' [bd_collect()]. To save the result in disk use the broader function
#' [bd_write()] or its derivatives [bd_write_csv()] or [bd_write_rds()] to
#' save, respectively in `.csv` or `.rds` format.
#'
#'
#' @param table String in the format `(dataset_name)`.`(table_name)`. You can optionally input a project before the dataset name.
#'
#' @param billing_project_id a string containing your billing project id.
#' If you've run [set_billing_id()] then feel free to leave this empty.
#'
#' @param query_project_id The project name at GoogleBigQuery. By default
#' `basedosdados`. You do not need to inform this if project is uset
#' on `table` parameter.
#'
#' @return A `lazy tibble`, which can be handled (almost) as if were a local
#' database. After satisfactorily handled, the result must be loaded into
#' memory using [bd_collect()] or written to disk using [bd_write()] or its
#' derivatives.
#'
#' @seealso [bd_collect()], [bd_write()], [bd_write_rds()], [bd_write_rds()],
#' [bigrquery::src_bigquery]
#'
#' @export
#'
#' @importFrom DBI dbConnect
#' @importFrom rlang abort inform
#' @importFrom dplyr everything select tbl
#' @importFrom stringr str_count
#'
#' @examples
#'
#' \dontrun{
#'
#' # set project billing id
#' basedosdados::set_billing_id("avalidprojectbillingid")
#'
#' # connects to the remote table I want
#' base_sim <- bdplyr("br_ms_sim.municipio_causa_idade")
#'
#' # connects to another remote table
#' municipios <- bdplyr("br_bd_diretorios_brasil.municipio")
#'
#' # explore data
#' base_sim %>%
#'   dplyr::glimpse()
#'
#' # use normal `{dplyr}` operations
#' municipios %>%
#'   head()
#'
#' # filter
#' base_sim_acre <- base_sim %>%
#'  dplyr::mutate(ano = as.numeric(ano)) %>%
#'   dplyr::filter(sigla_uf == "AC", ano >= 2018)
#'
#' municipios_acre <- municipios %>%
#'   dplyr::filter(sigla_uf == "AC") %>%
#'   dplyr::select(id_municipio, municipio, regiao)
#'
#'
#' # join
#' base_junta <- base_sim_acre %>%
#'   dplyr::left_join(municipios_acre,
#'                    by = "id_municipio")
#'
#' # tests whether the result is satisfactory
#' base_junta
#'
#' # collect the result
#' base_final <- base_junta %>%
#'   basedosdados::bd_collect()
#'
#' # alternatively, write in disk the result
#'
#' base_final %>%
#'   basedosdados::bd_write_rds(path = "data-raw/data.rds")
#'
#'}

#TODO: usar bigrquery::bq_has_token() para checar se atenticou no Google
#TODO: tentar autenticar em silencio com bigrquery::bq_auth(email = <email>)
# conferir: https://github.com/r-dbi/bigrquery/blob/main/R/bq-auth.R
#TODO: usar uma vignette pra explicar a compatibilidade

# bdplyr ------------------------------------------------------------------

bdplyr <- function(
  table,
  billing_project_id = basedosdados::get_billing_id(),
  query_project_id = "basedosdados") {

  # checking billing id

  if(billing_project_id == FALSE) {

    rlang::abort("You haven't set a Project Billing Id. Use the function `set_billing_id` to do so.")

  }

  # checking table and query_project_id param
  # if table has 2 dots consider that project is already informed
  # if it has 1 dots, glue project and table param
  # otherwise is a invalid table name

  how_many_dots <- stringr::str_count(string = table,
                                      pattern = "\\.")

  if (how_many_dots < 1 | how_many_dots > 2) {

    rlang::abort("`table` is invalid. Please use the pattern: `<project_name>.<dataset_name>.<table_name>` OR `<dataset_name>.<table_name>' if the parameter `query_project_id' is informed.")
  }

  # checks if is a valid query_project_id string

  if (!rlang::is_string(query_project_id)) {

    rlang::abort("`query_project_id` must be a string.")

  }

  if (how_many_dots == 1) {

    table_full_name <- glue::glue("{query_project_id}.{table}")

  }

  if (how_many_dots == 2 ) {

    table_full_name <- table
  }


  # checks if is a valid table at Google Big Query

  if(bigrquery::bq_table_exists(table_full_name) == FALSE) {

    rlang::abort(glue::glue("The table {table_full_name} doesn't have a valid name or was not found at {query_project_id}."))

  }

  # creates the connection

  con <- DBI::dbConnect(
    drv = bigrquery::bigquery(),
    project = query_project_id,
    billing = billing_project_id)

  class(con) <- "BaseDosDadosConnection"

  # calls the connection through dplyr and keeps it in a objects
  tibble_connection <- dplyr::tbl(con, table_full_name)


  # checks if the connection was successfully
  if (is_tbl_lazy(tibble_connection) == TRUE) {

    # prevent returning an empty table
    tibble_connection <- tibble_connection %>%
      dplyr::select(dplyr::everything())

    rlang::inform(glue::glue("Successfully connected to table `{table_full_name}`."))
    return (tibble_connection)

  } else {

    rlang::abort(glue::glue("It was not possible to connect to the remote table `{table_full_name}`"))
  }

}


# bdcollect ---------------------------------------------------------------

#' Collects the results of a remote table called via `bdplyr()`
#'
#' @description
#'
#' After [bdplyr()] is used to create the remote connection, this function
#' allows you to collect the result of the manipulations carried out with
#' the {dplyr}'s verbs and thus use it in local memory completely.
#'
#' Alternatively, you can also save to disk directly using [bd_write()]
#' function or its derivatives: [bd_write_csv()] or [bd_write_rds()].
#'
#' @param .lazy_tbl A variable that contains a database that was previously
#' connected through the [bdplyr()] function. Tipically, it will be called
#' after performing the desired operations with the `{dplyr}` verbs.
#'
#' @param billing_project_id a string containing your billing project id.
#' If you've run [set_billing_id()] then feel free to leave this empty.
#'
#' @param show_query If TRUE will show the SQL query calling [dplyr::show_query()].
#' Is useful for diagnosing performance problems.
#'
#' @return A tibble.
#' @export
#'
#' @examples
#' \dontrun{
#'
#' # setup billing
#'  basedosdados::set_billing_id("billing-project-id")
#'
#'  # select a cool database at Base dos Dados
#' bd_table <- basedosdados::bdplyr(
#'   "basedosdados.br_sp_gov_ssp.ocorrencias_registradas")
#'
#' # quick look
#' bd_table %>%
#'   dplyr::glimpse()
#'
#'  # filter, select and group the remote data
#' bd_ssp <-  bd_table %>%
#'   dplyr::filter(ano >= 2019) %>%
#'   dplyr::select(ano, mes, homicidio_doloso) %>%
#'   dplyr::group_by(ano, mes)
#'
#'  # make some plots
#' library(ggplot2)
#'
#' bd_ssp %>%
#'  # collect the data to continue the analisis
#'  basedosdados::bd_collect() %>%
#'   dplyr::summarise(homicidios_sum = sum(homicidio_doloso,
#'                                          na.rm = TRUE)) %>%
#'   ggplot(aes(x = mes, y = homicidios_sum, fill = ano)) +
#'   geom_col(position = "dodge")
#'
#'
#' }

bd_collect <- function(
  .lazy_tbl,
  billing_project_id = basedosdados::get_billing_id(),
  show_query = FALSE) {

  # check if billing_is is valid

  if(billing_project_id == FALSE) {

    rlang::abort("You haven't set a Project Billing Id. Use the function `set_billing_id` to do so.")

  }

  # checks if .lazy_tbl is able to be collected

  if(is_tbl_lazy(.lazy_tbl) == FALSE) {

    rlang::abort("`.lazy_tbl' should be a lazy tibble.")
  }

  # show que generated query - useful for debugging

  if (show_query) {

    rlang::inform(glue::glue("The following query will be executed:"))

    dplyr::show_query(.lazy_tbl)

  }

  # collect from the remote table
  # uses a previous select everything to avoid return empty
  collected_table <- .lazy_tbl %>%
    dplyr::select(dplyr::everything()) %>%
    dplyr::collect()

  # checks if is a tibble
  if (inherits(collected_table, "tbl_df") == FALSE) {

    rlang::abort("Error collecting results.")

  }

  return(collected_table)

}

# bd_write ----------------------------------------------------------------

#' Writes the result of operations with [bdplyr()] to disk
#'
#' @description
#'
#' Writes a remote table to disk that was called via {bdplyr}.
#' It will collect the data and write to disk in the chosen format.
#' You will only need this function if you have not yet collected the data
#' using the [bd_collect()].
#'
#' The comprehensive function [bd_write()] takes as a parameter `.write_fn`,
#' which will be the name of some function (without parentheses) capable of
#' writing a tibble to disk.
#'
#' As helpers, the [bd_write_rds()] and [bd_write_csv()] functions make it
#' easier to write in these formats, more common in everyday life, calling
#' writing functions from `{readr}` package.
#'
#' @param .lazy_tbl A lazy tibble, tipically the output of [bdplyr()].
#'
#' @param .write_fn A function for writing the result of a tibble to
#' disk. Do not use () afther the function's name, the function *object* should be passed. Some functions the user might consider are:
#' [writexl::write_xlsx], [jsonlite::write_json], [foreign::write.dta],
#' [arrow::write_feather], etc.
#'
#' @param path String containing the path for the file to be created.
#' The desired folders must already exist and the file should normally end with
#' the corresponding extension.
#'
#' @param overwrite FALSE by default. Indicates whether the local file should
#' be overwritten if it already exists. Use with care.
#'
#' @param compress For [bd_write_rds()] only. Compression method to use: "none"
#' (default), "gz" ,"bz", or "xz", in ascending order of compression.
#' Remember that the higher the compression, the smaller the file size on disk,
#' ut also the longer the time to load the data. See also: [readr::write_rds()].
#'
#' @param ... Parameters passed to the `.write_fn` function.
#'
#'
#' @return String containing the path to the created file.
#' @export
#'
#' @importFrom scales number_bytes
#'
#' @importFrom writexl write_xlsx
#'
#' @name bd_write
#' @examples
#' \dontrun{
#'
#'  cool_db <- basedosdados::
#'
#' # setup billing
#'basedosdados::set_billing_id("MY-BILLING-ID")
#'
#'# connect with a Base dos Dados db
#'
#'cool_db_ssp <- basedosdados::bdplyr(
#'  "basedosdados.br_sp_gov_ssp.ocorrencias_registradas")
#'
#'# subset the data
#'my_subset <- cool_db_ssp %>%
#'  dplyr::filter(ano == 2021, mes == 04)
#'
#'# write it in csv - generic function
#'
#'basedosdados::bd_write(.lazy_tbl = my_subset,
#'                       .write_fn = write.csv,
#'                       "data-raw/ssp_subset.csv"
#')
#'
#'# write in .xlsx
#'basedosdados::bd_write(.lazy_tbl = my_subset,
#'                       .write_fn = writexl::write_xlsx,
#'                       "data-raw/ssp_subset.xlsx"
#')
#'
#'# using the derivatives functions
#'# to csv
#'basedosdados::bd_write_csv(.lazy_tbl = my_subset,
#'                           "data-raw/ssp_subset2.csv"
#')
#'
#' #' # to rds
#' basedosdados::bd_write_rds(.lazy_tbl = my_subset,
#'                            "data-raw/ssp_subset.rds"
#' )
#'
#' # to rds - with compression
#' basedosdados::bd_write_rds(.lazy_tbl = my_subset,
#'                            "data-raw/ssp_subset2.rds",
#'                            compress = "gz"
#' )
#'
#' # to rds - with HARD compression
#' basedosdados::bd_write_rds(.lazy_tbl = my_subset,
#'                            "data-raw/ssp_subset3.rds",
#'                            compress = "xz"
#' )
#'
#' ## using other write functions
#'
#' # json
#' basedosdados::bd_write(.lazy_tbl = my_subset,
#'                        .write_fn = jsonlite::write_json,
#'                        "data-raw/ssp_subset.json"
#' )
#'
#' # dta
#' basedosdados::bd_write(.lazy_tbl = my_subset,
#'                        .write_fn = foreign::write.dta,
#'                        "data-raw/ssp_subset.dta")
#' )
#'
#' # feather
#' basedosdados::bd_write(.lazy_tbl = my_subset,
#'                        .write_fn = arrow::write_feather,
#'                        "data-raw/ssp_subset.feather"
#' )
#' }

bd_write <- function(
  .lazy_tbl,
  .write_fn,
  path,
  overwrite = FALSE,
  ...) {

  # check if the path already exists
  # in this case, check if overwrite = TRUE
  if (fs::file_exists(path) & overwrite == FALSE) {

    "{path} already exists. Set the `overwrite` argument to TRUE if you want to overwrite it." %>%
      glue::glue() %>%
      rlang::abort()

  }

   #  checks if .lazy_tbl is able to be collected
  if (is_tbl_lazy(.lazy_tbl) == FALSE) {

    # if is it not able to collect but is a tibble anyway warns that is not
    # necessary to use this function
    if (tibble::is_tibble(.lazy_tbl)) {

      rlang::abort("The table does not seem to need to be collected. Save using traditional writing functions like `write.csv'.")

      # if is not a tibble actually is a mistake and we should abort

      } else {

      rlang::abort("It was not possible to collect the remote table.")

    }

  }

  # collect the results
   collected_table <- bd_collect(.lazy_tbl)

  # write the results using the indicated function
  rlang::call2(.write_fn, collected_table, path, ...) %>%
    rlang::eval_bare()

  # checks if the writing process was successfull

  if (fs::file_exists(path)) {

    file_size <-  scales::number_bytes(file.info(path)$size, units = "si")

    msg <- glue::glue("The file was successfully saved ({file_size})")

    rlang::inform(msg)

    invisible(path)

  } else {

    rlang::abort("Failed to save the file.")

  }

}


# bd_write_rds e bd_write_csv ---------------------------------------------

#' @rdname bd_write
#' @importFrom scales number_bytes
#' @export
bd_write_rds <- function(
  .lazy_tbl,
  path,
  overwrite = FALSE,
  compress = "none",
  ...) {

  # check if is a valid path name
  if (stringr::str_detect(path, pattern = "(\\.rds)$") == FALSE) {
    rlang::abort("Pass a valid file name to argument `path`, include the '.rds' suffix.")
  }

  # check if the path already exists
  # in this case, check if overwrite = TRUE
  if (file.exists(path) & overwrite == FALSE) {
    rlang::abort("The file already exists. Use overwrite = TRUE if you want to overwrite it.")
  }

  # collect the remote results
  collected_table <- bd_collect(.lazy_tbl)


  # write the .rds file using {readr}

  readr::write_rds(x = collected_table,
                   file = path,
                   compress = compress,
                   ...)

  # checks if the writing process was successfully
  if (file.exists(path)) {

    file_size <-  scales::number_bytes(file.info(path)$size, units = "si")

    msg <- glue::glue("The file was successfully saved ({file_size})")

    rlang::inform(msg)

    invisible(path)
  } else {
    rlang::abort("Failed to save the file.")
  }

}

#' @rdname bd_write
#' @export
bd_write_csv <- function(
  .lazy_tbl,
  path,
  overwrite = FALSE,
  ...) {

  # check if the path already exists
  # in this case, check if overwrite = TRUE

  if (fs::file_exists(path) & overwrite == FALSE) {
    rlang::abort("The file already exists. Use overwrite = TRUE if you want to overwrite it.")
  }

  # collec the remote results
  collected_table <- bd_collect(.lazy_tbl)

  # calls the write function
  readr::write_csv(
    x = collected_table,
    file = path,
    ...)

  # checks if the writing process was successfully
  if (fs::file_exists(path)) {

    file_size <-  scales::number_bytes(file.info(path)$size, units = "si")

    msg <- glue::glue("The file was successfully saved ({file_size})")

    rlang::inform(msg)

    invisible(path)
  } else {
    rlang::abort("Failed to save the file.")
  }


}

# is_tbl_lazy -------------------------------------------------------------
# internal function to check if is a valid connection to be used

is_tbl_lazy <- function(tibble_connection) {
  rlang::inherits_any(
    x = tibble_connection,
    class =  c("tbl_BigQueryConnection",
               "tbl_dbi",
               "tbl_sql",
               "tbl_lazy")
  )

}
