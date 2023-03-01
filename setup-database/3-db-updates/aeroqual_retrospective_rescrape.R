# In February 2023 a retrospective rescrape of Aeroqual was run to obtain the MOMA
# calibrations from prior to their introduction. For every other company we had 
# the calibration values going back to the start of the study but we never made
# the rescrape for Aeroqual.
# This is also used as a sanity check that the current DB values are accurate.
library(tidyverse)
library(lubridate)
library(odbc)
library(data.table)
library(trelliscopejs)
library(DBI)

# Load new values from text
dir <- "~/Documents/quant_data/Aeroqual-rescrape"
fns <- list.files(dir, full.names = TRUE)
fns <- setNames(fns, basename(fns))
df_rescrape <- rbindlist(lapply(fns, fread), idcol="filename")
df_rescrape[, instrument := gsub("Aeroqual_", "", filename)]
df_rescrape[, instrument := gsub("_20.+\\.csv", "", instrument)]
df_rescrape[, filename := NULL]
df_rescrape[, version := ifelse(grepl("[_| ]cal", measurand), "MOMA", "cal1")]
df_rescrape[, measurand := gsub("[_| ]cal", "", measurand)]
setnames(df_rescrape, old=c("timestamp", "value"), new=c("time", "measurement"))
setcolorder(df_rescrape, c("time", "instrument", "measurand", "version", "measurement"))

# Read in current values from DB
con <- dbConnect(odbc(), "QUANT")
df_db <- tbl(con, "measurement") |>
            filter(instrument %in% c("AQY872", "AQY873A", "AQY875A2", "AQY874", "AQY875"),
                   time < "2021-03-04") |>
            select(time, instrument, measurand, version=calibrationname, measurement) |>
            collect() 
setDT(df_db)

# Looks like cal1 and OOB are swapped pre April 24th 2020
df_db[, version_swap := ifelse(version == 'out-of-box', 'cal1',
                               ifelse(version == 'cal1' & time < as_datetime("2020-04-24"), 'out-of-box', 
                                      version))]
comb_swap <- df_rescrape |>
            left_join(df_db |> select(-version), 
                      by=c("time", "instrument", "measurand", "version"="version_swap")) |>
            as.data.table()
comb_swap[, post_april := time >= as_datetime("2020-04-24")]
setnames(comb_swap, old=c("measurement.x", "measurement.y"), new=c("rescrape", "db"))
# This shows that after swapping cal1 and OOB prior to 24th April we get near perfect
# alignment from the rescraped values and the DB, i.e. demonstrating that these
# calibration names are indeed swapped
comb_swap2[version == 'cal1', 
     .(prop_same = mean(abs(db - rescrape) < 1e-6*100, na.rm=T)), 
     by=.(instrument, post_april)]

# Can observe this step change in a plot
df_db[ instrument == 'AQY872' & time >= '2020-04-20' & time < '2020-04-28' & measurand %in% c('NO2', 'O3', 'PM2.5')] |>
   ggplot(aes(x=time, y=measurement, colour=version)) +
        geom_line() +
        facet_wrap(~measurand, scales="free",
                   ncol=1) 

# Need to do 2 things
#  - 1. Swap cal1 and OOB prior to 2020-04-24
#  - 2. Add MOMA in retrospectively

# 1. Swap cal1 and OOB
# I manually inspected the data and confirmed that this mislabelling
# affects all 4 instruments
# Want to update all values of NO2, O3, PM2.5 & PM10 for all 4 instruments
measurands_to_swap <- expand_grid(instrument = c('AQY872', 'AQY873A', 'AQY874', 'AQY875'),
                                    measurand = c('NO2', 'O3', 'PM10', 'PM2.5'))
swap_cal_version <- function(instrument, measurand) {
    # OOB -> temp
    # cal1 -> OOB
    # temp -> cal1
    dbAppendTable(con, "sensorcalibration", 
                  tibble(instrument=instrument, measurand=measurand, sensornumber=1,
                         calibrationname = 'temp', dateapplied=as_datetime("2019-12-16 00:00:00")))
    dbExecute(con, "UPDATE measurement SET calibrationname = 'temp' 
                    WHERE instrument = ? AND measurand = ? AND 
              calibrationname = 'out-of-box' AND time < '2020-04-24'",
              params=list(instrument, measurand))
    dbExecute(con, "UPDATE measurement SET calibrationname = 'out-of-box' 
                    WHERE instrument = ? AND measurand = ? AND 
              calibrationname = 'cal1' AND time < '2020-04-24'",
              params=list(instrument, measurand))
    dbExecute(con, "UPDATE measurement SET calibrationname = 'cal1' 
                    WHERE instrument = ? AND measurand = ? AND 
              calibrationname = 'temp' AND time < '2020-04-24'",
              params=list(instrument, measurand))
    dbExecute(con, "DELETE FROM sensorcalibration
                    WHERE instrument = ? AND measurand = ? AND 
              calibrationname = 'temp'",
              params=list(instrument, measurand))
}
for (i in 1:nrow(measurands_to_swap)) {
    cat(sprintf("On row %d/%d\n", i, nrow(measurands_to_swap)))
    swap_cal_version(measurands_to_swap$instrument[i],
                     measurands_to_swap$measurand[i])
}

###### 2. Add MOMA values to DB
to_upload <- df_rescrape[ version == 'MOMA' & time < as_datetime('2020-04-22')]
to_upload[, sensornumber := 1]
setnames(to_upload, old=c("version"), new=c("calibrationname"))
setcolorder(to_upload, c("instrument", "measurand", "sensornumber", "calibrationname", "time", "measurement"))

batch_size <- 1e5
n_obs <- nrow(to_upload)
start_points <- seq(from=1, to=n_obs, by=batch_size)
i <- 1
for (start in start_points) {
    cat(sprintf("On batch %d/%d\n", i, length(start_points)))
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "measurement", to_upload[start:end, ])
    i <- i + 1
}

dbDisconnect(con)