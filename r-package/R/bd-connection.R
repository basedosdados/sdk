#' Base dos dados specific connection to BigQuery
#'
#' Implements a connection class to prevent a bug caused when trying to use
#' BigQueryConnection. The bug is described in Pull Request #1563. Once fixed,
#' this class and its method should probably be removed.
#'
#' @export
#' @importFrom methods setClass

methods::setClass("BaseDosDadosConnection", contains = "BigQueryConnection")

#' @rdname BaseDosDadosConnection-class
#'
#' @param con A BaseDosDadosConnection object.
#' @importFrom dbplyr dbplyr_edition
#' @export

dbplyr_edition.BaseDosDadosConnection <- function(con) 2L
