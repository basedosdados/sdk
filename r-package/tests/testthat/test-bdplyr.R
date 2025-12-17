if (!basedosdados::get_billing_id()) {

  basedosdados::set_billing_id(readline("Insira um billing project id: "))

}

require(magrittr)

basedosdados::bdplyr("br_tse_eleicoes.candidatos") %>%
  dplyr::filter(ano == 2014, sigla_partido %in% c("DEM", "PSOL")) %>%
  dplyr::select(ano, cpf, titulo_eleitoral, sigla_partido, numero, nome, genero) ->
  example_data


test_that("bd_colllect works", {

  testthat::skip_on_cran()

  testthat::expect_s3_class(example_data, "tbl_lazy")

  example_data %>%
    dplyr::group_by(sigla_partido) %>%
    dplyr::summarise(contagem = n()) %>%
    basedosdados::bd_collect() ->
    summarise_test_data

  testthat::expect_s3_class(summarise_test_data, "tbl_df")
  testthat::expect_gt(nrow(summarise_test_data), 0)

})

test_that("bd_write works", {

  testthat::skip_on_cran()

  (bd_write_test_params <- tibble::tibble(
    extension = c("csv", "xlsx", "json", "dta"),
    write_fn = list(write.csv, writexl::write_xlsx, jsonlite::write_json, foreign::write.dta),
    paths = file.path(tempdir(), glue::glue("bd_write_fn_{extension}.{extension}"))))

  bd_write_test_params %$%
    purrr::walk2(
      .x = paths,
      .y = write_fn,
      .f =
        ~ basedosdados::bd_write(
          .lazy_tbl = example_data,
          .write_fn = .y,
          overwrite = TRUE,
          path = .x) %>%
        fs::file_exists() %>%
        testthat::expect_true())

})


test_that("bd_write_csv works", {

  testthat::skip_on_cran()

  tempfile_csv <- file.path(tempdir(), "bd_write_csv.csv")

  example_data %>%
    basedosdados::bd_write_csv(
      tempfile_csv,
      overwrite = TRUE)

  tempfile_csv %>%
    fs::file_exists() %>%
    testthat::expect_true()

})

test_that("bd_write_rds", {

  testthat::skip_on_cran()

  tempfile1 <- file.path(tempdir(), "first_bdplyr_test.rds")

  example_data %>%
    basedosdados::bd_write_rds(tempfile1, overwrite = TRUE)

  tempfile1 %>%
    fs::file_exists() %>%
    testthat::expect_true()

  tempfile2 <- file.path(tempdir(), "second_bdplyr_test.rds")

  example_data %>%
    basedosdados::bd_write_rds(
      tempfile2,
      compress = 'gz',
      overwrite = TRUE)

  tempfile2 %>%
    fs::file_exists() %>%
    testthat::expect_true()

  tempfile3 <- file.path(tempdir(), "third_bdplyr_test.rds")

  example_data %>%
    basedosdados::bd_write_rds(
      tempfile3,
      compress = 'xz',
      overwrite = TRUE)

  tempfile3 %>%
    fs::file_exists() %>%
    testthat::expect_true()


})

testthat::test_that("page_size does not corrupt results", {

  testthat::skip_on_cran()

  example_data %>%
    basedosdados::bd_collect() ->
    page150

  example_data %>%
    basedosdados::bd_collect() ->
    page50

  waldo::compare(page150, page50) %>%
    length() %>%
    testthat::expect_equal(0)

})

test_that("bd_write handles ...", {

  testthat::skip_on_cran()

  temp_dotdotdot <- file.path(tempdir(), "dotdotdot.delim")

  example_data %>%
    basedosdados::bd_write(
      temp_dotdotdot,
      .write_fn = readr::write_delim,
      overwrite = TRUE,
      delim = " - ")

  temp_dotdotdot_data <- readr::read_delim(
    temp_dotdotdot,
    delim = " - ",
    col_types = readr::cols(
      ano = readr::col_integer(),
      cpf = readr::col_character(),
      titulo_eleitoral = readr::col_character(),
      sigla_partido = readr::col_character(),
      numero = readr::col_character(),
      nome = readr::col_character(),
      genero = readr::col_character()))

  # these two tibbles should differ only in class
  # temp_dotdotdot_data has a spec_tbl_df class because of its col specification
  # and an attribute called "spec" describing it
  # only these two differences should be detected

  waldo::compare(
    bd_collect(example_data),
    temp_dotdotdot_data) %>%
    length() %>%
    testthat::expect_equal(2)

})
