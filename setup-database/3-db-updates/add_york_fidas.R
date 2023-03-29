# We ran a FIDAS alongside the existing BAM at Fishergate from 
# 2022-05-17 (it's still there as of March 2023) to investigate whether the LCS appear more accurate when
# using a FIDAS as the ground-truth (which is a similar measurement technique)
# than the BAM.
library(tidyverse)
library(lubridate)
library(odbc)
library(data.table)
library(tidytable)
library(DBI)

##### Read clean data from scrape
fidas_dir <- "~/GoogleDrive/WACL/QUANT/analysis/York-FIDAS-BAM-comparison/fidas-data/"
fns <- list.files(fidas_dir, pattern="*.txt", full.names = TRUE)
df_fidas <- rbindlist(lapply(fns, fread))
flag_cols <- list("status flow"="flow",
                  "status coinc."="coincidence",
                  "status pumps"="pumps",
                  "status w.-statation"="weather station",
                  "status IADS"="IADS",
                  "status calib."="calibration",
                  "status LED"="LED",
                  "status op.-modus"="operating mode")

df_fidas <- df_fidas |>
                mutate(
                 dt := sprintf("%s %s", date, time),
                 time := as_datetime(dt, format="%m/%d/%Y %I:%M:%S %p")
                ) |>
                select(time, PM1, PM2.5, PM4, PM10, RelHumidity=rH, Temperature=T,
                       all_of(names(flag_cols)))

# Create a new column as a CSV list of all flags that were high at this timepoint
# From the documentation it sounds like these are all warnings
# https://www.csagroupuk.org/wp-content/uploads/2016/04/Palas-UK-Report-Final-with-Manuals-080316.pdf
for (col in names(flag_cols)) {
    df_fidas[[col]] <- factor(df_fidas[[col]], levels=c(0, 1),
                              labels=c("", flag_cols[[col]]))
}
df_fidas <- df_fidas |>
                mutate(reason = paste(`status flow`, 
                                           `status coinc.`,
                                           `status pumps`,
                                           `status w.-statation`,
                                           `status IADS`,
                                           `status calib.`,
                                           `status LED`,
                                           `status op.-modus`,
                                           sep=","
                                           ),
                       reason = gsub(",,+", ",", reason),
                       reason = gsub("^,", "", reason),
                       reason = gsub(",$", "", reason),
                       flagtype = ifelse(reason != '', 'Warning', '')) |>
                select(-all_of(names(flag_cols))) |>
                pivot_longer(-c(time, reason, flagtype),
                             names_to="measurand",
                             values_to="measurement") |>
                filter(!is.na(measurement)) |>
                mutate(sensornumber = 1,
                       instrument = 'FIDAS_York',
                       calibrationname = 'Unratified') |>
                select(instrument, measurand, sensornumber, calibrationname, time, measurement, reason, flagtype) |>
                as.data.table()

##### Upload to DB
con <- dbConnect(odbc(), "QUANT")

# instrument
dbAppendTable(con,
              "instrument",
              df_fidas |> distinct(instrument) |> mutate(instrumenttypeid = 2) |> as.data.table())

# referenceinstrument
dbAppendTable(
    con,
    "referenceinstrument",
    df_fidas |>
        distinct(instrument) |>
        mutate(
            instrumenttypeid = 2,
            manufacturer = "Palas",
            model = "FIDAS200"
        ) |>
        as.data.table()
)

# deployment
dbAppendTable(
    con,
    "deployment",
    df_fidas |>
        distinct(instrument) |>
        mutate(
            location = 'York',
            start = as_datetime("2022-05-17"),
            finish = as_datetime("2023-03-31")
        ) |>
        as.data.table()
)

# sensor
dbAppendTable(con,
              "sensor",
              df_fidas |> distinct(instrument, measurand, sensornumber) |> as.data.table())

# sensorcalibration
dbAppendTable(
    con,
    "sensorcalibration",
    df_fidas |>
        distinct(instrument, measurand, sensornumber, calibrationname) |>
        mutate(dateapplied = as_datetime("2022-05-17")) |>
        as.data.table()
)

# measurement
batch_size <- 1e5
n_obs <- nrow(df_fidas)
start_points <- seq(from = 1, to = n_obs, by = batch_size)
i <- 1
for (start in start_points) {
    cat(sprintf("On batch %d/%d\n", i, length(start_points)))
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "measurement", df_fidas[start:end,] |> select(-reason, -flagtype) |> as.data.table())
    i <- i + 1
}

# flag
df_flag <- df_fidas |> 
             filter(flagtype != '') |>
             select(-measurement)
batch_size <- 1e5
n_obs <- nrow(df_flag)
start_points <- seq(from = 1, to = n_obs, by = batch_size)
i <- 1
for (start in start_points) {
    cat(sprintf("On batch %d/%d\n", i, length(start_points)))
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "flag", df_flag[start:end,] |> as.data.table())
    i <- i + 1
}

dbDisconnect(con)