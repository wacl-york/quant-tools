# collate_EI_flags.R
# ~~~~~~~~~~~~~~~~~~
#
# Scrapes the flags from Environmental Instrument's raw JSON files 
# from the API and adds them to the DB.
# This is based off collate_AQMesh_flags.R since both use ACOEM's API
# And are AQMesh based devices
# But should be a lot simpler since there are no periods of overlapping
# measurements where a calibration has been backdated.
library(tidyverse)
library(lubridate)
library(data.table)
library(jsonlite)
library(parallel)
library(odbc)

con <- dbConnect(odbc(), "QUANT")

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
# Read raw files
########################
# All measurements are in the same folder

# No flagged values in initial out-of-box
INPUT_DIR <- "~/Documents/quant_data/Raw_wider/"
df <- read_all_files(INPUT_DIR, "EI")

# Add calibration names using the dates in the DB
# Multi-step process as can't do in a single join
# Firstly take cartesian product of all flags with all cals
cal_versions <- tbl(con, "lcsinstrument") |>
        filter(company == 'EI') |>
        inner_join(tbl(con, "sensorcalibration"), by="instrument") |>
        select(instrument, measurand, sensornumber, calibrationname, dateapplied) |>
        collect() |>
        setDT()
df_with_cals <- df[cal_versions, on=.(instrument, measurand), allow.cartesian=TRUE]

# Then filter to the contemporary cal version for each timepoint
# Have to revert to tidyverse here, can't get it working in data.table.
# For some reason this only returns cal2 values
# df_with_cals[ time >= dateapplied, 
#              .(calibrationname), 
#              by=.(time, measurand, instrument, flag, measurement, sensornumber)]
df_with_cals <- df_with_cals |>
    filter(time >= dateapplied) |>
    group_by(time, instrument, measurand, sensornumber) |>
    slice_max(dateapplied) |>
    ungroup() |>
    setDT()
                                          
# Rename and reorder cols to be consistent with the DB
setnames(df_with_cals,
         old=c("flag"),
         new=c("reason"))
df_with_cals[, measurement := NULL]
df_with_cals[, dateapplied := NULL]
setcolorder(df_with_cals, c("instrument", "measurand", "sensornumber", "calibrationname", "time", "reason"))
# Rename measurands to be consistent with db
df_with_cals[ measurand == 'HUM', measurand := 'RelHumidity']
df_with_cals[ measurand == 'TEMP', measurand := 'Temperature']

# Sometimes PM2.5 doesn't have the fullstop
df_with_cals[, reason := gsub("PM25", "PM2\\.5", reason)]

# Combine flags when have multiple readings
df_with_cals <- df_with_cals[, 
                             .(reason=paste(reason, collapse=",")),
                             by=.(instrument, measurand, sensornumber, calibrationname, time)]

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
df_with_cals[, reason := remove_duplicates_reorder_remove_rebasing(reason) ]

# Set flag types, which are mostly Warnings introduced by the
# ACOEM API, except for the following flags that indicate measurements
# should be discarded:
#  - Deliquescence
#  - Depletion event
#  - Stabilising
df_with_cals[, flagtype := 'Warning']
df_with_cals[ grepl("Stabilising", reason), flagtype := 'Error']
df_with_cals[ grepl("Deliquescence", reason), flagtype := 'Error']
df_with_cals[ grepl("Depletion", reason), flagtype := 'Error']

###########################
# Upload flags to DB
###########################
dups <- df_with_cals[, .N, by=c('time', 'instrument', 'measurand', 'sensornumber', 'calibrationname')][N > 1]
stopifnot(nrow(dups) == 0)

# Separate the period that is already flagged in DB when MAQS went down
# Will update those flags rather than insert
maqs_down <- df_with_cals[ time > as_datetime("2022-03-01 08:00:00") & time < as_datetime("2022-03-01 16:00:00")]
df_with_cals <- df_with_cals[ time <= as_datetime("2022-03-01 08:00:00") | time >= as_datetime("2022-03-01 16:00:00")]
    
batch_size <- 1e5
n_obs <- nrow(df_with_cals)
start_points <- seq(from=1, to=n_obs, by=batch_size)
i <- 1
for (start in start_points) {
    cat(sprintf("On batch %d/%d\n", i, length(start_points)))
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "flag", df_with_cals[start:end, ])
    i <- i + 1
}

# Now update the maqs_down flags
# These are entirely CO2 Rebasing flags
maqs_down
db_maqs_flags <- tbl(con, "flag") |>
                    filter(time > as_datetime("2022-03-01 08:00:00"),
                           time < as_datetime("2022-03-01 16:00:00"),
                           instrument %in% c('AQM1', 'AQM2', 'AQM3')) |>
                    collect()
# The existing Flags comprises "MAQS power problem", so I will append the Rebasing to this
db_maqs_flags
dbSendQuery(con, "UPDATE flag SET reason = 'MAQS power problem,Rebasing' WHERE instrument IN ('AQM1', 'AQM2', 'AQM3') AND time > '2022-03-01 08:00:00' AND time < '2022-03-01 16:00:00' AND measurand = 'CO2'")

dbDisconnect(con)
