# add_missing_PurpleAir.R
# ~~~~~~~~~~~~~~~~~~~~~~~
# Author: Stuart Lacy
# Date: 2022-01-24
#
# Populates the SQLite database with PurpleAir data that was erroneously
# missing during the DB creation.
# Mostly uses the original code from 1-preprocess-lcs-data/collate_PurpleAir.R
# and 2-populate-db/2_populate_database.R to firstly pre-process the PA data that
# is available in the GoogleDrive folder and then to organise it in the same manner
# as the DB that currently resides in the DB.
# This allows a quicker comparison to find data that isn't in the database that should be.

library(tidyverse)
library(lubridate)
library(reticulate)
library(odbc)
library(RSQLite)
library(data.table)

MEASUREMENT_COLS <- c(
    "O3",
    "NO2",
    "NO",
    "CO",
    "CO2",
    "PM1",
    "PM2.5",
    "PM10",
    "Temperature",
    "RelHumidity"
)

CLEAN_DIR <- "/home/stuart/Documents/quant_data/Clean"
DB_FN <- "/home/stuart/Documents/quant_data/quant.db"

ld <- import("load_data")
df_file <- ld$load_data(CLEAN_DIR,
                        companies="PurpleAir",
                        end="2021-06-30",
                        subset=NULL) %>%
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()

fahrenheit_to_celsius <- function(degF) (degF - 32) / 1.8

# Convert Temp to C and indicate second PM sensors in device ID rather than measurand
df_file <- df_file %>%
    mutate(value = ifelse(measurand == "Temperature_F", fahrenheit_to_celsius(value), value),
           measurand = gsub("Temperature_F", "Temperature", measurand),
           device = ifelse(grepl("_b$", measurand), sprintf("%s_b", device), device),
           measurand = gsub("_b$", "", measurand)
           ) %>%
    filter(measurand %in% c("PM1", "PM2.5", "PM10", "Temperature", "RelHumidity"))

# Pre-process to be in same format as DB tables
df_file <- df_file[ !is.na(value)]
df_file[ , version := "out-of-box"]
df_file[, manufacturer := NULL ]

dt_wide <- dcast(df_file, timestamp + version + device ~ measurand, value.var="value")
# Add any measurands that may not have had measurements for
cols_to_add <- setdiff(MEASUREMENT_COLS, colnames(dt_wide))
for (col in cols_to_add) {
    dt_wide[ , (col) := NA ]
}

# Add deployments
con <- dbConnect(RSQLite::SQLite(), DB_FN)
deployments <- tbl(con, "deployments") %>% 
    collect() %>%
    mutate(start = as_datetime(start),
           end=as_datetime(end))

dt_wide <- dt_wide %>%
    inner_join(deployments, by="device") %>%
    filter(timestamp >= start, timestamp <= end) %>%
    select(-start, -end) %>%
    setDT()

# Format in same order as DB
setcolorder(dt_wide, c("timestamp", "device", "location", "version", MEASUREMENT_COLS))
setnames(dt_wide, old=MEASUREMENT_COLS, new=gsub("\\.", "", MEASUREMENT_COLS))

# Retrieve data that is already in the DB
df_db <- tbl(con, "lcs") %>%
    filter(device %like% "PA%",
           timestamp < '2021-07-01') %>%
    collect() %>%
    setDT()
df_db[, timestamp := as_datetime(timestamp)]
common_cols <- colnames(df_db)[1:14]
df_db[, in_db := 1]
dt_wide[, in_gdrive := 1]

# Sanity check: find rows that aren't present in the files but are in the DB
# Pass this test, good to clarify
db_outer <- dt_wide[df_db, on=common_cols]
db_outer %>% count(in_gdrive)
db_outer %>% count(in_db)

# Find rows in Google Drive but not in DB
# Have 800K rows that are in Google Drive but not DB
db_outer_2 <- df_db[dt_wide, on=common_cols]
db_outer_2 %>% count(in_gdrive)
db_outer_2 %>% count(in_db)

missing_rows <- db_outer_2[ is.na(in_db) ]
missing_rows[, in_db := NULL ]
missing_rows[, in_gdrive := NULL ]

# Add to DB, both the main LCS table and the one
# for just the most recent dataset version
dbAppendTable(con, "lcs_raw", missing_rows)
missing_rows[, version := NULL]
dbAppendTable(con, "lcs_latest_raw", missing_rows)

dbDisconnect(con)
