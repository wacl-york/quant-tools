library(tidyverse)
library(odbc)
library(lubridate)

new <- read_csv("~/GoogleDrive/WACL/BOCS/Calibration/Analyses/data/Reference/London/Max_dropoff_20220503/HP_2021_Quant.csv")
colnames(new) <- c("time", "NO", "NOy", "O3", "NO2", "PM10", "PM25")
new$time <- as_datetime(new$time, format="%d-%m-%y %H:%M", tz="UTC")

min(new$time)

# Ok let's get old data from DB from 30th Dec onwards and compare
con <- dbConnect(odbc(), "QUANT")
old <- tbl(con, "ref") |>
        filter(location == "London",
               time >= "2020-12-30") |>
        collect() |>
        select(time, measurand, measurement) |>
        pivot_wider(names_from=measurand, values_from=measurement)

new_overlap <- new |>
    filter(time >= min(old$time),
           time <= max(old$time)) |>
    arrange(time)

# Have perfect overlap for these 2 days
c(nrow(new_overlap), nrow(old))

# Species in common are: NO2, NO, O3, PM2.5, PM10
## NO2
summary(old$NO2 - new_overlap$NO2)

# So let's compare on a species by species level
# The same!
## NO2
summary(old$NO2 - new_overlap$NO2)
## NO are the same
summary(old$NO - new_overlap$NO)
## Subtle differences with Ozone, I won't update for now
summary(old$O3 - new_overlap$O3)
## No differences in PM
summary(old$PM2.5 - new_overlap$PM25)
summary(old$PM10 - new_overlap$PM10)

# Ok let's add these data to the db!
# TODO add instrument names
new_to_upload <- new |> filter(time > max(old$time)) |>
                    pivot_longer(-time, names_to="measurand",
                                 values_to="measurement") |>
                    mutate(measurand=gsub("PM25", "PM2.5", measurand),
                           sensornumber=1, calibrationname="Ratified",
                           instrument=sprintf("Ref_London_%s", measurand))
dbAppendTable(con, "measurement", new_to_upload)
