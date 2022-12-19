#
# add_manchester_solarradiation.R
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Adds solar radiation to the DB. This data isn't available
# on CEDA and has been manually provided as an archive of monthly
# CSV files.
library(tidyverse)
library(odbc)
library(lubridate)

# Read data
input_dir <- "Data/manchester-solarradiation-dec2022/"
fns <- list.files(input_dir, full.names = TRUE)
df <- map_dfr(fns, read_csv, show_col_types=FALSE)

# Setup the associated metadata in the DB
con <- dbConnect(odbc(), "QUANT")

# This is the first set of solar radiation measurements
dbAppendTable(con, "measurand",
              data.frame(measurand="SolarRadiation",
                         units="W/m2"))
dbAppendTable(con, "instrument",
              data.frame(instrument="FIRS_Manchester",
                         instrumenttypeid=2))
dbAppendTable(con, "referenceinstrument",
              data.frame(instrument="FIRS_Manchester",
                         instrumenttypeid=2))
dbAppendTable(con, "sensor",
              data.frame(instrument="FIRS_Manchester",
                         measurand="SolarRadiation",
                         sensornumber=1))
dbAppendTable(con, "sensorcalibration",
              data.frame(instrument="FIRS_Manchester",
                         measurand="SolarRadiation",
                         sensornumber=1,
                         calibrationname="Ratified",
                         dateapplied=min(df$datetime)))
dbAppendTable(con, "deployment",
              data.frame(instrument="FIRS_Manchester",
                         location="Manchester",
                         start=min(df$datetime),
                         finish=as_datetime("2022-10-31 23:59:59")))

# Upload data
df <- df |>
    mutate(instrument = "FIRS_Manchester",
           measurand = "SolarRadiation",
           sensornumber=1,
           calibrationname="Ratified") |>
    select(instrument, 
           measurand,
           sensornumber,
           calibrationname,
           time=datetime,
           measurement=`Solar Actinic Flux (W/m2)`,
           flag=qc_Flags) |>
    filter(time < as_datetime("2022-11-01"))
batch_size <- 1e5
n_obs <- nrow(df)
start_points <- seq(from=1, to=n_obs, by=batch_size)
i <- 1
for (start in start_points) {
    cat(sprintf("On batch %d/%d\n", i, length(start_points)))
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "measurement", df[start:end, setdiff(colnames(df), "flag")])
    i <- i + 1
}

# Upload flags
flags <- df |> 
            filter(flag == 2) |>
            mutate(reason="", 
                   flagtype="Error") |>
            select(-flag, -measurement)
n_obs <- nrow(flags)
start_points <- seq(from=1, to=n_obs, by=batch_size)
i <- 1
for (start in start_points) {
    cat(sprintf("On batch %d/%d\n", i, length(start_points)))
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "flag", flags[start:end, ])
    i <- i + 1
}
