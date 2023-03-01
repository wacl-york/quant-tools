# add_york_ref.R
# ~~~~~~~~~~~~~~
# Author: Stuart Lacy
# Date: 2022-01-24
# Updated: 2023-02-23
#
# Populates the Postgres database with O3,NO2, and NO reference data from the 
# York Fishergate site.
library(tidyverse)
library(data.table)
library(lubridate)
library(odbc)

con <- dbConnect(odbc(), "QUANT")

##### Ozone
gdrive_dir <- "~/GoogleDrive/WACL/BOCS/Calibration/Analyses/data/Reference/York/"
# Ozone and NO2 have been manually pulled from the instrument and uploaded to Google Drive
raw_dir <- paste0(gdrive_dir, "Ozone/raw")
all_files <- list.files(raw_dir, full.names = TRUE)
df_o3 <- map_dfr(setNames(all_files, all_files), read_csv, col_names=c("Ozone", "Temp", "Pressure", "Flow", "Date", "Time"), col_types=c(rep("d", 4), "D", "t"),
                 .id="fn")
# Parse datetime
df_o3 <- df_o3 %>%
    mutate(timestamp = sprintf("%s %s", Date, Time),
           timestamp = as_datetime(timestamp, format="%d/%m/%y %H:%M:%S", tz="UTC")) %>%
    select(timestamp, Ozone, Temp, Pressure, Flow, Date, Time) |>
    filter(!is.na(timestamp))
# Round down to nearest minute and average in case have multiple
df_o3$timestamp <- floor_date(df_o3$timestamp, "1 min")
df_o3 <- df_o3 %>%
    group_by(timestamp) %>%
    summarise(O3 = mean(Ozone, na.rm=T),
              Temperature = mean(Temp, na.rm=T),
              Pressure = mean(Pressure, na.rm=T)) %>%
    ungroup()

# Have manually checked and there are no outliers in the Ozone data

##### NO2
df_no2 <- read_csv(paste0(gdrive_dir, "NOx/raw/19 Oct 2020_10 Aug 2022.csv"), skip = 3, 
                   col_names = c("timestamp", "time2", "NOxDiff", "NOx", "NO2", "NO")) |>
    separate(timestamp, into=c("date", "time"), sep=" ") %>%
    separate(time, into=c("hour", "min"), sep=":") %>%
    mutate(hour = sprintf("%02d", as.integer(hour)),
           second = "00") %>%
    unite(time, c("hour", "min", "second"), sep=":") %>%
    unite(timestamp, c("date", "time"), sep=" ") %>%
    # Round down to nearest minute and average in case have multiple
    mutate(timestamp = as_datetime(timestamp, format="%m/%d/%Y %H:%M:%S", tz="UTC"),
           floor_date(df_no2$timestamp, "1 min")) |>
    group_by(timestamp) %>%
    summarise(
        NOx = mean(NOx, na.rm=T),
        NO2_raw = mean(NO2, na.rm=T),
        NO = mean(NO, na.rm=T)) %>%
    ungroup() |>
    # Converting to ppb from mV
    mutate(
        NOx = NOx * 100,
        NO2_raw = NO2_raw * 100,
        NO = NO * 10,
        NO2_manual=NOx - NO,
    )


###### Combining streams
# Combine datasets into 1 data frame
comb <- full_join(df_o3, df_no2, by="timestamp")
comb$location <- "York"

# Get required column names from DB
con <- dbConnect(RSQLite::SQLite(), DB_FN)

required_columns <- tbl(con, "ref_raw") %>%
    colnames()
cols_to_add <- setdiff(required_columns, colnames(comb))
for (col in cols_to_add) {
    comb[[col]] <- NA
}
comb <- comb[, required_columns]
setDT(comb)

# Unfortunately this NOx stream is filled with calibration noise that hasn't been removed yet
# so a copy will be made for the duplicates table with all NO2 and NO set as NA
# For evidence, have a look at plotting 2021-03-22 to 2021-03-23 and observe the calibration
# artefacts from NO2 after midday on the 23rd and for NO there are calibration spikes
# just after midnight
comb_corrections <- copy(comb)
comb_corrections[, NO := NA]
comb_corrections[, NO2 := NA]

###### Upload to DB

dbAppendTable(con, "ref_raw", comb)
dbAppendTable(con, "ref_corrections", comb_corrections)

dbDisconnect(con)

