# collate_AQMesh_flags.R
# ~~~~~~~~~~~~~~~~~~~~~~
#
# Scrapes the flags from AQMesh raw JSON files from the API
# and adds them to the DB.
# This requires a bit of manual cleaning to sort out the various 
# archives of backdated scrapes, and to align flags from the raw JSON
# with measurements that are in the DB
library(tidyverse)
library(lubridate)
library(data.table)
library(jsonlite)
library(parallel)
library(odbc)

INPUT_DIR <- "~/Documents/quant_data/one-off-downloads/extracted/"
flag_list_to_vector <- function(x) {
    sapply(x, function(x) {
        if (length(x) == 0) {
            ""
        } else {
            paste(x, collapse=", ")
        }
    })
}

read_aqmesh_json <- function(fn) {
    print(fn)
    raw <- fromJSON(fn)
    df <- if ('Headers' %in% names(raw)) {
        df <- data.frame(raw$Rows[2:nrow(raw$Rows), ])
        colnames(df) <- raw$Rows[1, ] 
        df |>
            select(time=ISO8601Timestamp, ends_with("-Scaled"), ends_with("-Flags")) |>
            mutate(time = as_datetime(time)) |>
            pivot_longer(-time, names_pattern = "(.+)-(.+)",
                         names_to=c("measurand", "type")) |>
            pivot_wider(names_from=type, values_from=value) |>
            mutate(Flags = flag_list_to_vector(Flags)) |>
            rename(measurement=Scaled, flag=Flags) |>
            setDT()
    } else if ('TBTimestamp' %in% names(raw)) {
        this_df <- rbindlist(raw$Channels)
        n_rows <- sapply(raw$Channels, nrow)
        this_df[, time := as_datetime(rep(raw$TBTimestamp, n_rows)) ]
        this_df <- this_df[, .(time, SensorLabel, Scaled, Flags)]
        this_df[, Flags := flag_list_to_vector(Flags)]
        setnames(this_df, old=c("SensorLabel", "Scaled", "Flags"),
                 new=c("measurand", "measurement", "flag"))
        this_df
    } else {
        # Assume have 1 entry per row
       df_t <- as_datetime(sapply(raw, function(x) x$Timestamp$Timestamp))
       n_rows <- sapply(raw, function(x) nrow(x$Channels))
       dt <- rbindlist(lapply(raw, function(x) x$Channels$Scaled))
       dt[, measurand := as.vector(sapply(raw, function(x) x$Channels$SensorLabel)) ]
       dt[, time := rep(df_t, n_rows) ]
       dt[, ValidPercentage := NULL]
       dt[, Flags := flag_list_to_vector(Flags)]
       setnames(dt, old=c("Reading", "Flags"),
                   new=c("measurement", "flag"))
       setcolorder(dt, c("time", "measurand", "measurement", "flag"))
       dt
    }
    df[ !is.na(measurement) & !(flag %in% c('', 'Valid'))]
}

read_all_files <- function(folder, company, start=NULL, end=NULL,
                           measurands=c('NO', 'NO2', 'O3', 'CO', 'CO2',
                                        'PM1', 'PM10', 'PM2.5',
                                        'HUM', 'TEMP'),
                           n_cores=4) {
    fns <- list.files(folder, pattern=sprintf("^%s_", company))
    dates <- as_date(str_extract(fns, "([0-9]{4}-[0-9]{2}-[0-9]{2})"))
    include <- rep(TRUE, length(fns))
    
    if (!is.null(start)) {
        include <- include & (dates >= as_date(start))
    }
    if (!is.null(end)) {
        include <- include & (dates <= as_date(end))
    }
    
    fns <- fns[include]
    
    # Remove hired devices
    fns <- fns[!grepl("AQM173", fns)]
    fns <- fns[!grepl("AQM801", fns)]
    
    fns <- setNames(paste(folder, fns, sep="/"), fns)
    
    dfs <- mclapply(fns, read_aqmesh_json, mc.cores=n_cores, mc.preschedule = FALSE)
    dfs <- Filter(is.data.table, dfs)
    df <- rbindlist(dfs, idcol = "filename")
    df[, instrument := gsub(sprintf("%s_", company), "", filename) ]
    df[, instrument := gsub("_.+", "", instrument) ]
    df <- df[ measurand %in% measurands]
    df[, filename := NULL ]
    df
}

########################
# Out-of-box
########################
# No flagged values in initial out-of-box
df_1 <- read_all_files(sprintf("%s/1-Archive/raw", INPUT_DIR), "AQMesh")
df_1[, source := '1 backup']
# But there are some in the later dataset
df_3 <- read_all_files(sprintf("%s/3-AQMesh/precalibrated_data/raw", INPUT_DIR), "AQMesh")
df_3[, source := '3 backup']

# Combine into a single dataset
oob <- rbindlist(list(df_1, df_3))

########################
# First cals (rebased)
########################
# The rebased cals (1st product) were applied in March 2021
# In archive #9 the rebased data was retrospectively added to the Clean folder and it would have been
# placed there by the daily scrape moving forwards.
# However, during 13 the rebased data was moved out of Clean to make way for the 2nd cal product
df_rebased_clean <- read_all_files("~/Documents/quant_data/Raw/", "AQMesh",
                                   end="2020-02-17")
df_rebased_clean[, source := 'raw before 2020-02-17']
                      
df_rebased <- read_all_files(sprintf("%s/13-AQMesh/Backup/Raw", INPUT_DIR), "AQMesh",
                             n_cores=8)
df_rebased[, source := '13 backup']

df_rebased <- rbindlist(list(df_rebased_clean, df_rebased))

########################
# Second cals (rebased)
########################
# The 2nd cal product is in the Clean folder since the retrospective scrape in 13
df_cals2 <- read_all_files("~/Documents/quant_data/Raw/", "AQMesh", start="2020-02-18", 
                           n_cores = 8)
df_cals2[, source := 'raw after 2020-02-18']

###########################
# Prepare data for upload
###########################
# Add calibration versions
oob[, calibrationname := "out-of-box" ]
df_rebased[, calibrationname := "cal1" ]
df_rebased[ measurand %in% c('HUM', 'TEMP'), calibrationname := 'out-of-box']
# For second cal, only NO, NO2, and O3 (plus CO2 for AQM388) were actually updated
# And CO2 for AQM388
df_cals2[, calibrationname := "cal1" ]
df_cals2[ measurand %in% c("NO", "NO2", "O3"), calibrationname := 'cal2' ]
df_cals2[ measurand == "CO2" & instrument == "AQM388", calibrationname := 'cal2']
df_cals2[ measurand %in% c('HUM', 'TEMP'), calibrationname := 'out-of-box']

# Combine into single table
to_upload <- rbindlist(list(df_rebased, oob, df_cals2))
# Add sensor number
to_upload[, sensornumber := 1 ]
# Rename and reorder cols to be consistent with the DB
setnames(to_upload,
         old=c("flag"),
         new=c("reason"))
setcolorder(to_upload, c("instrument", "measurand", "sensornumber", "calibrationname", "time", "reason", "source"))
# Rename measurands to be consistent with db
to_upload[ measurand == 'HUM', measurand := 'RelHumidity']
to_upload[ measurand == 'TEMP', measurand := 'Temperature']

to_upload[, measurement := as.numeric(measurement)]
# Sometimes PM2.5 doesn't have the fullstop
to_upload[, reason := gsub("PM25", "PM2\\.5", reason)]

# Combine multiple flags and reorder in consistent fashion
reorder_reasons <- function(x) {
    split <- str_split(x, ",")
    split <- lapply(split, trimws)
    split <- lapply(split, sort)
    sapply(split, paste, collapse=',')
}
to_upload[, reason := reorder_reasons(reason) ]
# This leads to some duplicate rows that can now remove
to_upload <- to_upload[ !duplicated(to_upload, by=c("instrument", "measurand",
                                                    "sensornumber", "calibrationname",
                                                    "time", "reason", "measurement"))]

# Combine flags when have multiple readings
to_upload <- to_upload[, .(reason=paste(reason, collapse=",")),
                       by=.(instrument, measurand, sensornumber, calibrationname, time, measurement)]

# Remove duplicates and remove Rebasing flag if Rebased flag is present
remove_rebasing <- function(x) {
    if ('Rebased' %in% x) {
        setdiff(x, 'Rebasing')
    } else {
        x
    }
}
remove_duplicates_reorder_remove_rebasing <- function(x) {
    split <- str_split(x, ",")
    split <- lapply(split, trimws)
    split <- lapply(split, unique)
    split <- lapply(split, remove_rebasing)
    split <- lapply(split, sort)
    sapply(split, paste, collapse=',')
}
to_upload[, reason := remove_duplicates_reorder_remove_rebasing(reason) ]

# Set flag types, which are mostly Warnings introduced by the
# ACOEM API, except for the following flags that indicate measurements
# should be discarded:
#  - Deliquescence
#  - Depletion event
#  - Stabilising
to_upload[, flagtype := 'Warning']
to_upload[ grepl("Stabilising", reason), flagtype := 'Error']
to_upload[ grepl("Deliquescence", reason), flagtype := 'Error']
to_upload[ grepl("Depletion", reason), flagtype := 'Error']

################################
# Download AQMesh Measurements
################################
# The Flags calibration versions don't 100% line up with the measurements in the DB
# This is because there was evidently a time when I backed up the Clean
# folder but not the Raw folder.
# As such as I'll take the measurements in the DB as the gold-standard
# and link the Flagged values to them to obtain the correct calibration
# version so they can be added to the DB
# Scrape all AQMesh data, batched per year to save memory
con <- dbConnect(odbc(), "QUANT")
scrape_year <- function(year) {
    start <- sprintf("%d-01-01 00:00:00", year)
    end <- sprintf("%d-12-31 23:59:59", year)
    tbl(con, "measurement") |>
        filter(time >= start, time <= end,
               instrument %in% c('AQM388', 'AQM389', 'AQM390', 'AQM391'),
               measurand %in% c('CO2', 'NO', 'NO2', 'O3', 'PM1', 'PM2.5', 'PM10')) |>
        collect() |>
        saveRDS(sprintf("AQMesh_db_%d.rds", year))
    
}
scrape_year(2019)
scrape_year(2020)
scrape_year(2021)
scrape_year(2022)
dbDisconnect(con)

###########################
# Upload flags to DB
###########################
# For each year join the flags and measurements 
# and sort any obvious conflicts with calibration names
round2 = function(x, digits) {
    posneg = sign(x)
    z = abs(x)*10^digits
    z = z + 0.5 + sqrt(.Machine$double.eps)
    z = trunc(z)
    z = z/10^digits
    z*posneg
}
con <- dbConnect(odbc(), "QUANT")
for (year in 2019:2022) {
    cat(sprintf("On year %d\n", year))
    meas <- readRDS(sprintf("AQMesh_db_%d.rds", year))
    setDT(meas)
    
    this_flags <- to_upload[ year(time) == year]
    year_upload <- this_flags |> left_join(meas, by=c("instrument", "measurand", "sensornumber", "time"))
    # Use cal name from measurements in the DB by default
    year_upload[, calibrationname := calibrationname.y ]
    # Except when the measurements don't match up
    year_upload[ measurement.x != measurement.y, calibrationname := NA]
    # Take the DB calibration name when measurement differences arise from FP precision
    year_upload[ is.na(calibrationname) & abs(measurement.x - measurement.y) < .Machine$double.eps,
                 calibrationname := calibrationname.y]
    # Or in some situations the cleaned data has been rounded to 1dp
    year_upload[ is.na(calibrationname) & abs(round(measurement.x, 1) - round(measurement.y, 1)) < .Machine$double.eps, calibrationname := calibrationname.y]
    year_upload[ is.na(calibrationname) & abs(measurement.x - measurement.y) <= 0.06+.Machine$double.eps, calibrationname := calibrationname.y]
    
    # Remove flags where don't have corresponding measurements in DB
    year_upload <- year_upload[ !is.na(measurement.y)]
    # Remove flags that couldn't match up calibration names with a measurement
    year_upload <- year_upload[ !is.na(calibrationname)]
    
    # Ensure have no duplicates
    dups <- year_upload[, .N, by=c('time', 'instrument', 'measurand', 'sensornumber', 'calibrationname')][N > 1]
    stopifnot(nrow(dups) == 0)
    
    # Duplicates might be inevitable here if a calibration product didn't actually update anything
    # or if a measurement didn't change, then a flag might still be needed
    year_upload <- unique(year_upload, by=c("instrument", "measurand", "sensornumber", "calibrationname", "time", "reason", "flagtype"))[, .(instrument, measurand, sensornumber, calibrationname, time, reason, flagtype)]
    
    batch_size <- 1e5
    n_obs <- nrow(year_upload)
    start_points <- seq(from=1, to=n_obs, by=batch_size)
    i <- 1
    for (start in start_points) {
        cat(sprintf("On batch %d/%d\n", i, length(start_points)))
        end <- min(n_obs, start + batch_size - 1)
        dbAppendTable(con, "flag", year_upload[start:end, ])
        i <- i + 1
    }
}
dbDisconnect(con)