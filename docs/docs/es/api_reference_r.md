# R

Esta API está compuesta solamente de módulos para **solicitud de datos**, es
decir, descarga y/o carga de datos del proyecto en tu entorno de
análisis.
Para realizar la **gestión de datos** en Google Cloud, busca las funciones
en la API de [línea de comandos](../api_reference_cli) o en [Python](../api_reference_python/#classes-gerenciamento-de-dados).

La documentación completa se encuentra en la página CRAN del proyecto, y
sigue abajo.

!!! Info "Toda la documentación del código abajo está en inglés"

<object data="https://cran.r-project.org/web/packages/basedosdados/basedosdados.pdf" type="application/pdf" width="700px" height="700px">
    <embed src="https://cran.r-project.org/web/packages/basedosdados/basedosdados.pdf">
        <p>Este navegador no soporta PDFs. Por favor descarga el PDF para verlo: <a href="https://cran.r-project.org/web/packages/basedosdados/basedosdados.pdf">Descargar PDF</a>.</p>
    </embed>
</object>

## ¡Ups, hubo un error! ¿Y ahora qué?
Los principales errores encontrados en el paquete de Base de los Datos en Rstudio se derivan de dos factores:

    * Autenticación

    * Versión del paquete `dbplyr`

Por lo tanto, si aparece algún error, por favor, primero intenta verificar si está relacionado con estos dos factores.

### Autenticación
La mayoría de los errores de nuestro paquete están relacionados con problemas de autenticación. El paquete `basedosdados` requiere que el usuario proporcione todas las autenticaciones solicitadas por la función `basedosdados::set_billing_id`, incluso aquellas que aparecen como opcionales. Por eso, es necesario estar atento si marcaste todas las casillas de selección cuando Rstudio muestra esta pantalla en el navegador:

![Capturar](https://user-images.githubusercontent.com/26544494/190700064-1326a74c-8de0-4254-a562-32f9aa10ae07.PNG)

**Ten en cuenta que es necesario marcar incluso las dos últimas "casillas", que aparecen como opcionales**. Si olvidaste marcarlas, todas las otras funciones del paquete no funcionarán posteriormente.

Si ya te has autenticado con autorización incompleta, es necesario repetir el proceso de autentificación. Puedes hacer esto ejecutando `gargle::gargle_oauth_sitrep()`. Deberás verificar la carpeta donde están guardadas las autenticaciones de tu R, entrar en esta carpeta y eliminar la referente a Google Cloud/Bigquery. Hecho esto, al ejecutar `basedosdados::set_billing_id` podrás autenticarte nuevamente.

Mira qué simple es:

![gif_gargle](https://user-images.githubusercontent.com/62671380/194094167-99dadbd7-f7de-46f9-ac88-fb464e646e6c.gif)

Realizados todos estos procedimientos, es muy probable que los errores anteriores no ocurran más.

### Versión del paquete `dbplyr`
Otro error común está relacionado con el uso de la función `basedosdados::bdplyr`. Nuestro paquete en R fue construido utilizando otros paquetes disponibles en la comunidad. Esto significa que las actualizaciones de estos paquetes pueden alterar su funcionamiento y generar efectos en cascada en otros paquetes desarrollados sobre ellos. En este contexto, nuestro paquete funciona solo con la versión 2.1.1 del paquete `dbplyr`, y **no** funciona con versiones posteriores.

Puedes verificar la versión de tu `dbplyr` ejecutando `utils::packageVersion("dbplyr")` en tu R. Si es superior a la versión 2.1.1, necesitas dar un _downgrade_ a la versión correcta. Para esto, puedes ejecutar `devtools::install_version("dbplyr", version = "2.1.1", repos = "http://cran.us.r-project.org")`.

### Otros errores
Caso los errores persistan, puedes abrir una _issue_ en nuestro Github clicando [aqui](https://github.com/basedosdados/mais/issues). También puedes visitar las _issues_ que ya fueron resueltas y están atribuídas con la etiqueta `R` en nuestro Github [aqui](https://github.com/basedosdados/mais/issues?q=is%3Aissue+is%3Aclosed).
