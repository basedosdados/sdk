
testthat::test_that("Basic dataset search", {

  testthat::skip_on_cran()

  result <- basedosdados::dataset_search("agua")

  testthat::expect_gt(nrow(result), 0)

  testthat::expect_s3_class(result, "tbl")

  # busca por água encontra o ana-atlas?
  result %>%
    dplyr::pull(dataset_name) %>%
    purrr::some(stringr::str_detect, pattern = "ana_atlas") %>%
    testthat::expect_true()

})

testthat::test_that("Different search terms yield different results", {

  testthat::skip_on_cran()

  ed <- dataset_search("educação")
  water <- dataset_search("água")

  ed %>%
    waldo::compare(ed) %>%
    length() %>%
    testthat::expect_equal(0)

  ed %>%
    waldo::compare(water) %>%
    length() %>%
    testthat::expect_gt(0)

})

testthat::test_that("Basic table column description works", {

  testthat::skip_on_cran()

  result <- basedosdados::get_table_columns("br_sp_alesp", "deputado")

  testthat::expect_equal(nrow(result), 12)

})

testthat::test_that("List tables of a dataset works", {

  testthat::skip_on_cran()

  result <- list_dataset_tables("br_sp_alesp")

  result %>%
    tibble::is_tibble() %>%
    testthat::expect_true()

})

testthat::test_that("Dataset description works", {

  testthat::skip_on_cran()

  description <- get_dataset_description("br_sp_alesp")

  testthat::expect_s3_class(description, "tbl")

  description %>%
    nrow() %>%
    testthat::expect_gt(0)


})

testthat::test_that("Table description works", {

  testthat::skip_on_cran()

  description <- get_table_description("br_sp_alesp", "deputado")

  testthat::expect_s3_class(description, "tbl")

  description %>%
    nrow() %>%
    testthat::expect_gt(0)

})

