# add_york_PM_reference_aurn
# ~~~~~~~~~~~~~~~~~~~~~~~~~~
# 
# Adds York PM reference data from the AURN to the DB.
#
# We originally recorded PM data live, scraped from the AURN website.
# However, these were unratified and frequently had errors.
# Enough time has passed now that the AURN data is in a healthier state, 
# so this script will retrospectively add PM measurements
# for the entire period we had devices in York (March 2020 - July 2022).

library(tidyverse)
library(odbc)
library(openair)
library(lubridate)

con <- dbConnect(odbc(), "QUANT")

START <- as_datetime("2020-03-01")
END <- as_datetime("2022-07-31")

df <- importAURN(site="YK11", year=2020:2022, 
                 data_type="hourly",
                 pollutant=c("pm2.5", "pm10"))
df <- df |>
    filter(date >= START, date <= END)

df <- df |>
        select(time=date, pm2.5, pm10) |>
        pivot_longer(-time, names_to="measurand", values_to="measurement") |>
        mutate(instrument = 'BAM_York',
               sensornumber=1,
               calibrationname="AURN",
               measurand=toupper(measurand)
               ) |>
    select(instrument, measurand, sensornumber, calibrationname, time, measurement) |>
    filter(!is.na(measurement))

# Insert all metadata first
dbAppendTable(con, "instrument", df |> distinct(instrument) |> mutate(instrumenttypeid=2))
dbAppendTable(con, "referenceinstrument", df |> distinct(instrument) |> mutate(instrumenttypeid=2))
dbAppendTable(con, "sensor", df |> distinct(instrument, measurand, sensornumber))
dbAppendTable(con, "sensorcalibration", df |> distinct(instrument, measurand, sensornumber, calibrationname) |> mutate(dateapplied=min(df$time)))
dbAppendTable(con, "measurement", df)
