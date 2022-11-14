# collate_Kunak_flags.R
# ~~~~~~~~~~~~~~~~~~~~~
#
# Scrapes the flags from Kunak's raw JSON files 
# from the API and adds them to the DB.
# These will be identified as status 'TI', as in 'Temporarily Invalidated' 
# by the automatic flagging system. Kunak expect a user to manually verify such
# samples, but this isn't part of the QUANT procedure.
# The flags also have error codes, with a human readable description listed here
# https://www.kunak.es/doc/08.Manuals/html/Kunak_Cloud_UserManual_EN.html#Data
library(tidyverse)
library(lubridate)
library(data.table)
library(jsonlite)
library(parallel)
library(odbc)

flag_list_to_vector <- function(x) {
    sapply(x, function(x) {
        if (length(x) == 0) {
            ""
        } else {
            paste(x, collapse=", ")
        }
    })
}

read_kunak_json <- function(fn) {
    print(fn)
    raw <- fromJSON(fn)
    if (length(raw) == 0) {
        return(NULL)
    }
    setDT(raw)
    raw <- raw[validation != 'T']
    setnames(raw, old=c('sensor_tag', 'value', 'ts', 'validation'),
             new=c('measurand', 'measurement', 'time', 'flag'))
    raw[, time := as_datetime(time/1000)]  # Raw timestamps are in ms
    raw[, reason := as.integer(reason)]
    raw
}

read_all_files <- function(folder, company, start=NULL, end=NULL,
                           measurands=c('CO GCc', 'CO2 GCc', 'NO GCc', 'NO2 GCc',
                                        'O3 GCc', 'PM1', 'PM2.5', 'PM10',
                                        'Pressure', 'Temp ext', 'Humidity ext',
                                        'Dew Point'),
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
    
    fns <- setNames(paste(folder, fns, sep="/"), fns)
    
    dfs <- mclapply(fns, read_kunak_json, mc.cores=n_cores, mc.preschedule = FALSE)
    df <- rbindlist(dfs, idcol = "filename")
    df[, instrument := gsub(sprintf("%s_", company), "", filename) ]
    df[, instrument := gsub("_.+", "", instrument) ]
    df <- df[ measurand %in% measurands]
    df[, filename := NULL ]
    df[, measurand := gsub(" GCc", "", measurand)]
    df
}

con <- dbConnect(odbc(), "QUANT")

########################
# Read raw files
########################
# All measurements are in the same folder
INPUT_DIR <- "~/Documents/quant_data/Raw_wider/"
df <- read_all_files(INPUT_DIR, "Kunak")

################################
# Link with measurements
# to get calibration versions
################################
# The most accurate way to get calibration versions is linking against the measurements in the DB,
# and this is simple for WPS participants since we only have 1 measurement per timepoint
# (no retrospectively applied cals)
meas <- tbl(con, "measurement") |> filter(instrument %in% c('AP1', 'AP2', 'AP3')) |> collect()
setDT(meas)
# Round to nearest minute to be consistent with DB
df[, time := floor_date(time, "1 minute")]
df[, measurement := as.numeric(measurement)]
df <- df[, .(measurement = mean(measurement, na.rm=T)), by=.(measurand, time, flag, reason, instrument)]

df_with_cals <- meas[ df, on=.(instrument, measurand, time)]

# Confirm the measurements themselves are the same so that are flagging the
# correct values
df_with_cals[, i.measurement := as.numeric(i.measurement)]
stopifnot(nrow(df_with_cals[ abs(measurement - i.measurement) > 0.01]) == 0)

# Remove unused measurement fields
df_with_cals[, measurement := NULL]
df_with_cals[, i.measurement := NULL]

# I'm curious how these O flags (standing for Corrected to a manual calibration)
# have arisen, the majority are when the data has been rescraped
# But there's also a couple from cal2 - maybe these were rescraped but just not flagged as such?
df_with_cals[, .N, by=.(calibrationname, flag)]

# Looking at the cal2 corrected values, these just occurred in 1 day, so maybe I did rescrape these
# after which point they had been manually corrected
df_with_cals[ calibrationname == 'cal2', .(min(time), max(time)), by=flag]

# Add in the text reason
reason_lut <- data.table(
    reason=c(
        0, 1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13
    ),
    reason_text=c(
        "n/a",
        "Calibration",
        "Maintenance / repair",
        "Sensor change",
        "Broken sensor",
        "Malfunction",
        "Stabilization period",
        "Installation / Relocation",
        "Natural event",
        "Remote supervision",
        "Other",
        "Out of range",
        "Miscalibrated"
    )
)

df_with_cals <- reason_lut[df_with_cals, on="reason"]
df_with_cals[, reason := NULL]

# Set flag type level, which is exclusion for all flags
# For manual corrections just want to provide some context
df_with_cals[, flagtype := 'Error']
df_with_cals[flag == 'O', flagtype := 'Info']
df_with_cals[flag == 'O', reason_text := 'Corrected to manual calibration']

# Rename and reorder cols to be consistent with the DB
setnames(df_with_cals,
         old=c("reason_text"),
         new=c("reason"))
df_with_cals[, flag := NULL]

setcolorder(df_with_cals, c("instrument", "measurand", "sensornumber", "calibrationname", "time", "reason"))

###########################
# Upload flags to DB
###########################
dups <- df_with_cals[, .N, by=c('time', 'instrument', 'measurand', 'sensornumber', 'calibrationname')][N > 1]
stopifnot(nrow(dups) == 0)

# Separate the period that is already flagged in DB when MAQS went down
# Will update those flags rather than insert
maqs_down <- df_with_cals[ time > as_datetime("2022-02-23 00:00:00") & time < as_datetime("2022-03-01 16:00:00")]
df_with_cals <- df_with_cals[ time <= as_datetime("2022-02-23 00:00:00") | time >= as_datetime("2022-03-01 16:00:00")]

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

# Now update the flags when the MAQS site was down
db_maqs_flags <- tbl(con, "flag") |>
                    filter(time > as_datetime("2022-02-23 00:00:00"),
                           time < as_datetime("2022-03-01 16:00:00"),
                           instrument %in% c('AP1', 'AP2', 'AP3')) |>
                    collect()
maqs_flags <- db_maqs_flags |>
    full_join(maqs_down, by=c('instrument', 'measurand', 'sensornumber', 'calibrationname', 'time'),
              suffix=c("_db", "_json"))
# Have more flagged values in the DB than there in the JSON files because 
# there was a conservative site-wide flagging applied during this period of downtime
maqs_flags |>
    filter(is.na(reason_db))
maqs_flags |>
    filter(is.na(reason_json))

# I just want to find times that fall under both the MAQS site-wide flag and also
# are flagged in the raw data, and update these values in the DB
# These are all errors so don't have any conflicts there
maqs_flags |>
    filter(!is.na(reason_json), !is.na(reason_db)) |>
    count(flagtype_db, flagtype_json)

# Instead want to concatenate the 2 reasons fields
maqs_flags_to_update <- maqs_flags |>
    filter(!is.na(reason_json), !is.na(reason_db)) |>
    mutate(reason = paste(reason_json, reason_db, sep=',')) |>
    select(instrument, measurand, sensornumber, calibrationname, time, reason, flagtype=flagtype_db)

# And update the DB
rows_update(tbl(con, "flag"), dbplyr::copy_inline(con, maqs_flags_to_update),
            by=c("instrument", "measurand", "sensornumber", "calibrationname", "time"),
            unmatched="ignore",
            in_place=TRUE)

dbDisconnect(con)