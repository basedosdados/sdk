{smcl}
{* *! version 16.0  23set2021}{...}
{vieweralsosee "" "--"}{...}
{marker title}{...}
{title:Title}

{phang}
{bf:bd_get_table_columns {hline 2}} Fetch the names, types and descriptions for the columns in the specified table. Prints information on screen. {p_end}

{marker syntax}{...}
{title:Syntax}

{phang}
{cmdab:bd_get_table_c:olumns}{cmd:,} {opth d:ataset_id(dataset_id)} {opth t:able_id(table_id)} {p_end}


{marker parameters}{...}
{title:Options}
{synoptline}
{phang}
{opth dataset_id(dataset_id)}: dataset_id that uniquely identifies a data set in the {it:Base dos Dados} platform. {p_end}

{phang}
{opth table_id(table_id)}: table_id that uniquely identifies a table within a data set in the {it:Base dos Dados} platform. {p_end}

{synoptline}

{marker description}{...}
{title:Description}

{pstd} To use {cmd:bd_get_table_columns} you must have Stata version 16+ and the Python {it:`basedosdados`} package installed and configured. If not, 
run {it:`pip install basedosdados`} and configure following the instructions at {browse "https://github.com/basedosdados/sdk/tree/master/stata-package":https://github.com/basedosdados/sdk/tree/master/stata-package}.

{pstd} Base dos Dados (BD) is a nonprofit with the mission to make access to high-quality data universal. You can support the project at {browse "https://apoia.se/basedosdados":https://apoia.se/basedosdados}. We also welcome collaboration and 
suggestions, so feel free to open issues on our Github page {browse "https://github.com/basedosdados/": https://github.com/basedosdados/} or get in touch 
via Discord {browse "https://discord.gg/Hfgq8BZts4": https://discord.gg/Hfgq8BZts4}.

{pstd}
Stata also has other commands for manipulating basedosdados's data; see
{manhelp bd_download B}, {manhelp bd_list_dataset_tables B}, {manhelp bd_list_datasets B}, 
{manhelp bd_read_sql B}, {manhelp bd_read_table B} or {manhelp bd_get_table_description B}.


{marker examples}{...}
{title:Examples}

  {hline}

  {cmd:. bd_get_table_columns, dataset_id("br_bd_diretorios_brasil") table_id("cnae_2")}

  {hline}

{marker author}{...}
{title: Author}

{phang2} Base dos Dados at {browse "https://basedosdados.org/":https://basedosdados.org/}. E-mail: rdahis@econ.puc-rio.br or isabellahelter@gmail.com
{p_end}

