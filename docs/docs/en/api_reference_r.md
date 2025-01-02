# R

This API consists only of modules for **data requests** (i.e., downloading and/or loading project data into your analysis environment).
For **data management** in Google Cloud, look for functions in the [command line](../api_reference_cli) or [Python](../api_reference_python/#classes-gerenciamento-de-dados) APIs.

The complete documentation can be found on the project's CRAN page, as shown below.

!!! Info "All documentation for the code below is in English"

<object data="https://cran.r-project.org/web/packages/basedosdados/basedosdados.pdf" type="application/pdf" width="700px" height="700px">
    <embed src="https://cran.r-project.org/web/packages/basedosdados/basedosdados.pdf">
        <p>This browser does not support PDFs. Please download the PDF to view it: <a href="https://cran.r-project.org/web/packages/basedosdados/basedosdados.pdf">Download PDF</a>.</p>
    </embed>
</object>

## Oops, got an error! What now?
The main errors found in the Base dos Dados package in RStudio are derived from two factors:

    * Authentication

    * Version of the `dbplyr` package

Therefore, if any error appears, please first try to check if it's related to these two factors.

### Authentication
Most errors in our package are related to authentication problems. The `basedosdados` package requires users to provide all authentications requested by the `basedosdados::set_billing_id` function, including those that appear as optional. Therefore, you need to be careful to check all selection boxes when RStudio displays this screen in the browser:

![Capture](https://user-images.githubusercontent.com/26544494/190700064-1326a74c-8de0-4254-a562-32f9aa10ae07.PNG)

**Note that you need to check even the last two "boxes" that appear as optional**. If you forgot to check them, all other package functions will not work afterward.

If you have already authenticated with incomplete authorization, you need to repeat the authentication process. You can do this by running `gargle::gargle_oauth_sitrep()`. You should check the folder where your R authentications are saved, enter this folder, and delete the one referring to Google Cloud/BigQuery. After that, when running `basedosdados::set_billing_id`, you can authenticate again.

See how simple it is:

![gif_gargle](https://user-images.githubusercontent.com/62671380/194094167-99dadbd7-f7de-46f9-ac88-fb464e646e6c.gif)

After completing all these procedures, it's very likely that the previous errors will no longer occur.

### Version of the `dbplyr` package
Another common error is related to the use of the `basedosdados::bdplyr` function. Our R package was built using other packages available in the community. This means that updates to these packages can change their functionality and generate cascade effects on other packages developed on top of them. In this context, our package only works with version 2.1.1 of the `dbplyr` package, and does **not** work with later versions.

You can check your `dbplyr` version by running `utils::packageVersion("dbplyr")` in R. If it's higher than version 2.1.1, you need to downgrade to the correct version. To do this, you can run `devtools::install_version("dbplyr", version = "2.1.1", repos = "http://cran.us.r-project.org")`.

### Other errors
If errors persist, you can open an issue on our Github by clicking [here](https://github.com/basedosdados/mais/issues). You can also visit the issues that have already been resolved and are tagged with the `R` label on our Github [here](https://github.com/basedosdados/mais/issues?q=is%3Aissue+is%3Aclosed).
