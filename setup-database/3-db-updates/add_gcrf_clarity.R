# We had 5 Clarity devices that were intended for the GRCF-BOCS project, but owing
# to Covid this portion of the project didn't go ahead. We therefore deployed
# these units at the York Fishergate site in March 2022 to collect more roadside data.
library(tidyverse)
library(lubridate)
library(odbc)
library(data.table)
library(DBI)

##### Read clean data from scrape
dir <- "~/Documents/quant_data/Clarity/"
fns <- list.files(dir, full.names = TRUE)
fns <- setNames(fns, basename(fns))
df <- rbindlist(lapply(fns, fread), idcol = "filename")
df[, instrument := gsub("ClarityGCRF_", "", filename)]
df[, instrument := gsub("_20.+\\.csv", "", instrument)]
df[, filename := NULL]
df[, version := 'out-of-box']
df[, sensornumber := 1]
setnames(
    df,
    old = c("timestamp", "value", "version"),
    new = c("time", "measurement", "calibrationname")
)
setcolorder(
    df,
    c(
        "instrument",
        "measurand",
        "sensornumber",
        "calibrationname",
        "time",
        "measurement"
    )
)

##### Upload to DB
con <- dbConnect(odbc(), "QUANT")

# instrument
dbAppendTable(con,
              "instrument",
              df |> distinct(instrument) |> mutate(instrumenttypeid = 1))

# lcsinstrument
internal_ids <- tribble(
    ~ instrument,
    ~ internalid,
    "Clar1",
    "A81YTSPH",
    "Clar2",
    "AQH7B7KR",
    "Clar3",
    "A56GC5DT",
    "Clar4",
    "ARYWM2LW",
    "Clar5",
    "A894WD0F",
    "Clar6",
    "A33XH4T1"
)
dbAppendTable(
    con,
    "lcsinstrument",
    df |>
        distinct(instrument) |>
        inner_join(internal_ids, by = 'instrument') |>
        mutate(
            instrumenttypeid = 1,
            study = "YorkClarity",
            company = "Clarity"
        )
)

# deployment
dbAppendTable(
    con,
    "deployment",
    df |>
        distinct(instrument) |>
        mutate(
            location = 'York',
            start = as_datetime("2022-03-28"),
            finish = as_datetime("2023-03-16")
        )
)

# sensor
dbAppendTable(con,
              "sensor",
              df |> distinct(instrument, measurand, sensornumber))

# sensorcalibration
dbAppendTable(
    con,
    "sensorcalibration",
    df |>
        distinct(instrument, measurand, sensornumber, calibrationname) |>
        mutate(dateapplied = as_datetime("2022-03-28"))
)

# measurement
batch_size <- 1e5
n_obs <- nrow(df)
start_points <- seq(from = 1, to = n_obs, by = batch_size)
i <- 1
for (start in start_points) {
    cat(sprintf("On batch %d/%d\n", i, length(start_points)))
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "measurement", df[start:end,])
    i <- i + 1
}

dbDisconnect(con)