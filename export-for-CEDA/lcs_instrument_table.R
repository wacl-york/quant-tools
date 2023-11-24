# Exports a table containing information about the LCS involved in this study.
library(tidyverse)
library(odbc)
con <- dbConnect(odbc(), "QUANT")

df <- tbl(con, "lcsinstrument") |>
    inner_join(tbl(con, "lcscompany")) |>
    inner_join(tbl(con, "sensor")) |>
    mutate(measurand = str_replace(measurand, "RelHumidity", "Relative Humidity")) |>
    filter(company != 'RLS') |>
    distinct(instrument, companyhumanreadable, modelname, url, internalid, study, measurand) |>
    group_by(instrument, companyhumanreadable, modelname, url, internalid, study) |>
    summarise(sensors = str_flatten(measurand, collapse=", ")) |>
    ungroup() |>
    select(study_id = instrument, study, manufacturer=companyhumanreadable, model=modelname, url, serial=internalid, sensors) |>
    collect() |>
    mutate(description = sprintf("Low-cost instrument (make: %s, model %s) measuring air quality as part of the QUANT %s study. Has sensors that measure: %s.", 
                                 manufacturer, model, ifelse(study == 'QUANT', 'main', study), sensors)) |>
    select(-sensors) |>
    arrange(manufacturer, study_id)
write_csv(df, "export-for-CEDA/lcs_instruments.csv")