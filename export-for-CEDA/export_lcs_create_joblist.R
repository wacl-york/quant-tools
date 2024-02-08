library(tidyverse)
library(DBI)
library(RPostgres)

LCS_DIR <- "~/scratch/QUANT/CEDA/lcs"
REFERENCE_DIR <- "~/scratch/QUANT/CEDA/reference"

con <- dbConnect(RPostgres::Postgres(), 
                 host="",
                 dbname="",
                 user="",
                 password=""
)

lcs_jobs <- tbl(con, "lcsinstrument") |>
    filter(company %in% c('RLS', 'QuantAQ')) |>
    inner_join(tbl(con, "deployment"), by="instrument") |>
    inner_join(tbl(con, "sensor"), by="instrument") |>
    filter(measurand != 'SO2') |>
    distinct(instrument, measurand, location) |>
    collect() |>
    mutate(measurand = gsub("PM.+", "PM", measurand),
           measurand = gsub("Temperature", "Met", measurand),
           measurand = gsub("Pressure", "Met", measurand),
           measurand = gsub("RelHumidity", "Met", measurand)) |>
    distinct(instrument, measurand, location) |>
    mutate(out_dir=LCS_DIR, version=1) |>
    select(instrument, measurand, location, out_dir, version)

write_csv(lcs_jobs, "lcs_job_list.csv")
dbDisconnect(con)
