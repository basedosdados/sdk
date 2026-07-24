

if (!basedosdados::get_billing_id()) {

  basedosdados::set_billing_id(readline("Insira um billing project id: "))

}


ex_query <- function(n = 5) {

  glue::glue("select * from br_tse_eleicoes.candidatos LIMIT {n}")

}

testthat::test_that(
  "download escreve arquivos recebendo uma query ou nome de tabela", {

  testthat::skip_on_cran()

  path <- file.path(tempdir(), "arquivo.csv")

    download(
      ex_query(),
      path = path)

  path %>%
    fs::is_file() %>%
    expect_true()

  path2 <- file.path(tempdir(), "arquivo2.csv")

  download(
    table = "br_ibge_populacao.municipio",
    path = path,
    .na = "999999999999")

  path %>%
    fs::is_file() %>%
    expect_true()

})

testthat::test_that(
  "download permite escolher o conteúdo de NAs", {

  testthat::skip_on_cran()

  path3 <- file.path(tempdir(), "arquivo3.csv")

  download(
    ex_query(),
    path = path3,
    .na = "999999")

  path3 %>%
    fs::is_file() %>%
    expect_true()

  download(
    ex_query(),
    path = path3,
    .na = "AUSENTE")

  path3 %>%
    fs::is_file() %>%
    expect_true()

})

testthat::test_that(
  "download permite escolher tamanho de página", {

  testthat::skip_on_cran()

  path4 <- file.path(tempdir(), "arquivo4.csv")

  download(
    ex_query(),
    path = path4)

  path4 %>%
    fs::is_file() %>%
    expect_true()

})

testthat::test_that(
  "download valida nomes de arquivos sem extensão", {

  testthat::skip_on_cran()

  expect_error(
    download(
      ex_query(),
      file.path(tempdir(), "arquivo")))

  })


testthat::test_that(
  "read_sql e read_table retornam um tibble com as propriedades esperadas", {

  testthat::skip_on_cran()

  testthat::expect_s3_class(read_sql(ex_query(1)), "tbl_df")

  read_sql(ex_query(5)) %>%
    nrow() %>%
    testthat::expect_gt(0)

  # read_table(table = "br_denatran_frota.uf_tipo") %>%
  #   nrow() %>%
  #   testthat::expect_gt(0)
  #
  # testthat::expect_s3_class(read_table(table = "br_denatran_frota.uf_tipo"), "tbl_df")

})

testthat::test_that(
  "read_sql e read_table falham com input inapropriado", {

  testthat::skip_on_cran()

  expect_error(read_sql(query = 123))
  expect_error(read_sql(query = TRUE))
  expect_error(read_sql(query = "sodfungosd"))
  expect_error(read_sql(query = letters))
  expect_error(read_sql())

})






