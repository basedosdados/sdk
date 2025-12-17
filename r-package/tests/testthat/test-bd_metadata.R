
testthat::test_that("Basic dataset search", {

  testthat::skip_on_cran()

  result <- basedosdados::dataset_search("agua")

  testthat::expect_s3_class(result, "tbl")
  testthat::expect_true(all(c("dataset_name", "dataset_tables", "url", "title") %in% names(result)))
  testthat::expect_gte(nrow(result), 0)

})

testthat::test_that("Different search terms yield different results", {

  testthat::skip_on_cran()

  ed <- dataset_search("educação")
  water <- dataset_search("água")

  testthat::expect_s3_class(ed, "tbl")
  testthat::expect_s3_class(water, "tbl")
  testthat::expect_true(all(c("dataset_name", "dataset_tables", "url", "title") %in% names(ed)))
  testthat::expect_true(all(c("dataset_name", "dataset_tables", "url", "title") %in% names(water)))
  ed %>%
    waldo::compare(ed) %>%
    length() %>%
    testthat::expect_equal(0)

})

testthat::test_that("Basic table column description works", {

  testthat::skip_on_cran()

  result <- basedosdados::get_table_columns("br_sp_alesp", "deputado")

  testthat::expect_s3_class(result, "tbl")
  testthat::expect_gte(nrow(result), 0)

})

testthat::test_that("List tables of a dataset works", {

  testthat::skip_on_cran()

  result <- list_dataset_tables("br_sp_alesp")

  result %>%
    tibble::is_tibble() %>%
    testthat::expect_true()
  testthat::expect_true(all(c("name", "description") %in% names(result)))

})

testthat::test_that("Dataset description works", {

  testthat::skip_on_cran()

  description <- get_dataset_description("br_sp_alesp")

  testthat::expect_s3_class(description, "tbl")
  testthat::expect_true(all(c("name", "title", "tables", "notes") %in% names(description)))
  testthat::expect_equal(nrow(description), 1)

})

testthat::test_that("Table description works", {

  testthat::skip_on_cran()

  description <- get_table_description("br_sp_alesp", "deputado")

  testthat::expect_s3_class(description, "tbl")
  testthat::expect_true(all(c("dataset_id", "table_id", "description", "columns") %in% names(description)))
  testthat::expect_equal(nrow(description), 1)

})

