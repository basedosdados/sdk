#' Abstraction of an http request to the API
#' @param endpoint which endpoint to request.
#' @param query querystring passed as argument to the GEt request.
#' @export
#' @examples
#' \dontrun{
#'
#' bd_request(
#'  endpoint = "dataset_search",
#'  query = list(
#'   resource_type = "bdm_table",
#'   q = search_term,
#'   page_size = 100))
#'
#' bd_request(
#'  endpoint = "bdm_dataset_show",
#'  query = list(
#'   dataset_id = dataset_id)
#'
#'}
#'@return A httr::response() object.


bd_request <- function(
  endpoint,
  query = list()) {

  base_url <- "https://basedosdados.org/api/3/action/bd_"
  target_endpoint <- paste0(base_url, endpoint)

  httr::GET(
    target_endpoint,
    encode = 'json',
    query = query) %>%
    httr::content() %>%
    purrr::pluck("result")

}
