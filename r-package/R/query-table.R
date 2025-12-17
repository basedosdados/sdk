
#
# Highly opinionated approach to querying - not exposed to end users yet
# @param ... comma-separated pairs of `<table_name>.<variable_name>`.
# @param table1 string containing the main table to be queried.
# @param table2 string containing an optional table to be joined.
# @param join a join specification. Acceptable values are `left`, `right`, `inner` and `full`.
# @param keys a 2-length vector containing strings with names of, respectively, `table1`'s foreign key and `table2`'s foreign key.
# @param page_size `bigrquery` internal.
# @param billing_project_id a string containing your billing project id. If you've run `set_billing_id` then feel free to leave this empty.
#
#
# @details Currently this is prototype and it won't be available to end users for a while.
#
# @examples
#
#
# \dontrun{
# query_table(
#   pib.id_municipio,
#   pib.PIB,
#   table1 = "basedosdados.br_ibge_pib.municipios as pib")
#
#   }
#
# @importFrom rlang ensyms is_null
# @importFrom purrr map reduce
# @importFrom glue glue
# @import bigrquery
#
#
# @return
#

rquery <- function(
  ...,
  table1,
  table2 = NULL,
  join = NULL,
  keys = NULL,
  page_size = 500,
  billing_project_id = get_billing_id()) {

  if(!rlang::is_null(table2) & rlang::is_null(join)) {

    rlang::abort('Provide a join specification to argument `join`. \n
                 Possible values include: `left`,`inner`,`full`,`right`.')

  }


  columns <- rlang::ensyms(...) %>%
    purrr::map(rlang::as_string) %>%
    purrr::reduce(paste, sep = ", ")


  single_table_query <- glue::glue(
    "
    SELECT
    {columns}
    FROM
    {table1}
  ")


  if(!rlang::is_null(table2)) {

    second_table_query <- glue::glue(
      "
      {toupper(join)} ON
      {purrr::pluck(keys, 1)} = {purrr::pluck(keys, 2)}
      ")

  } else {

    second_table_query <- ""

  }

  query <- paste0(single_table_query, second_table_query)

  bigrquery::bq_table_download(
    x = bigrquery::bq_project_query(
      billing_project_id,
      query),
    page_size = page_size)

}



