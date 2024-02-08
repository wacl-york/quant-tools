# Exports a table containing information about the LCS involved in this study.
library(tidyverse)
library(odbc)
con <- dbConnect(odbc(), "QUANT")

study_table <- tribble(
    ~study_old, ~study_new,
    "QUANT", "Main QUANT",
    "Wider Participation", "Wider Participation Study"
)

df <- tbl(con, "lcsinstrument") |>
    inner_join(tbl(con, "lcscompany")) |>
    inner_join(tbl(con, "sensor")) |>
    mutate(measurand = str_replace(measurand, "RelHumidity", "Relative Humidity")) |>
    distinct(instrument, companyhumanreadable, modelname, url, internalid, study, measurand) |>
    group_by(instrument, companyhumanreadable, modelname, url, internalid, study) |>
    summarise(sensors = str_flatten(measurand, collapse=", ")) |>
    ungroup() |>
    select(system_id = instrument, study, manufacturer=companyhumanreadable, model=modelname, url, serial=internalid, sensors) |>
    collect() |>
    inner_join(study_table, by=c("study"="study_old")) |>
    mutate(description = sprintf("Low-cost instrument (make: %s, model %s) measuring air quality as part of the QUANT %s study. Has sensors that measure: %s.", 
                                 manufacturer, model, ifelse(study == 'QUANT', 'main', study), sensors)) |>
    select(system_id, study=study_new, manufacturer, model, url, serial, description) |>
    arrange(manufacturer, system_id)
write_csv(df, "export-for-CEDA/lcs_instruments.csv")
