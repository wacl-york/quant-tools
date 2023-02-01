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

# Add 2022 data
df_2022 <- read_csv("~/GoogleDrive/WACL/BOCS/Calibration/Analyses/data/Reference/London/Max_email_20230124/HOP quant 2022.csv")
colnames(df_2022) <- gsub("@HP1", "", colnames(df_2022))
df_2022 <- df_2022 |> 
            rename(time = `Date&Time`,
                   PM2.5=PM25,
                   NOy=NOY) |>
            mutate(time=as_datetime(time, format="%d-%m-%y %H:%M", tz="UTC"))
# Current max time in DB is 2022-03-05 23:00 and we have minutely
tbl(con, "ref") |>
    filter(location == 'London',
           measurand == 'O3') |>
    arrange(desc(time)) |> head(20)

# We have data from 2022-03-01 but 15 minutely
# Let's check the data overlaps
old <- tbl(con, "ref") |>
    filter(location == 'London', time >= as_datetime("2022-03-01")) |>
    collect() |>
    mutate(time = floor_date(time, "15 minutes")) |>
    group_by(time, measurand) |>
    summarise(measurement = mean(measurement, na.rm=T)) |>
    ungroup() 
new <- df_2022 |>
        filter(time >= min(old$time), time <= max(old$time)) |>
        pivot_longer(-time, names_to="measurand", values_to="measurement")

# Most differences are very small - less than 1ppb
# NO has a day or so where the new data has been increased by a few ppb
old |>
    rename(old=measurement) |>
    inner_join(new |> rename(new=measurement), by=c("time", "measurand")) |>
    mutate(diff = old - new) |>
    ggplot(aes(x=time, y=diff, colour=measurand)) +
        geom_line()
# And in the context of the full time-range, the differences in NO are proportionally very low
old |>
    mutate(dataset="old") |>
    rbind(new |> mutate(dataset="new")) |>
    ggplot(aes(x=time, y=measurement, colour=dataset)) +
        geom_line() +
        facet_wrap(~measurand, scales="free")

# So I can add these data to the dataset for now, maybe from the maximum time onwards
# But it would be nice to get minutely when available!
# Prepare for DB upload
to_upload <- df_2022 |>
    filter(time > max(old$time)) |>
    pivot_longer(-time, names_to="measurand", values_to="measurement") |>
    mutate(sensornumber=1, calibrationname="Unratified2022",
           instrument=sprintf("Ref_London_%s", measurand),
           instrument = gsub("PM.+", "PM", instrument)) |>
    select(instrument, measurand, sensornumber, calibrationname, time, measurement) |>
    filter(!is.na(measurement))

# Firstly add Unratified calibration versions
dbAppendTable(con, "sensorcalibration", 
              to_upload |> 
                  distinct(instrument, measurand, sensornumber, calibrationname) |>
                  mutate(dateapplied = as_datetime("2023-01-24"))
                  )
# Now upload data
dbAppendTable(con, "measurement", to_upload)

