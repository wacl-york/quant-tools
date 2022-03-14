# add_manchester_ref_2022.R
# ~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Adds the unratified reference data from the Manchester supersite for 
# the year 2022 to the database.
#
# NB: CO2 IS NOT BEING ADDED TO THE DB
# This is because there are some extreme negative spikes in the CO2
# data that I don't trust and would need to be confirmed by someone more
# familiar with this data.
#
# The raw unratified data must be downloaded locally from the Shared Drive
# QUANT/Data/Reference/Manchester, and it is all the files that are *not* in the 
# ratified sub-directory
# There should be a MAQS file per month.
# 
# In the BOCS/Calibration/Analyses/data/Reference/Manchester/validating-2022-maqs dir
# I have run some analyses looking at how reliable this new dataset is
library(tidyverse)
library(data.table)
library(RSQLite)
library(lubridate)
library(odbc)

DB_FN <- "~/Documents/quant_data/quant.db"
con <- dbConnect(SQLite(), DB_FN)
local_raw_dir <- "~/GoogleDrive/WACL/BOCS/Calibration/Analyses/data/Reference/Manchester/extract_2022/"
maqs_fns <- Sys.glob(file.path(local_raw_dir, "MAQS*"))

dt <- rbindlist(lapply(maqs_fns, fread))

# Form timestamp from the 2 separate columns
dt[, timestamp := paste(Date, Time, sep=" ")]
dt[, timestamp := as_datetime(dt$timestamp, format="%d/%m/%Y %H:%M:%S")]

# Subset to cols of interest
setnames(dt,
         old=c("NO (ppb)", "NO2 (ppb)", "Ozone (ppb)", "CO (ppb)", "CO2 (ppm)", "PM1 (ug/m3)", "PM2.5 (ug/m3)", "PM10 (ug/m3)", "Temperature (deg C)", "Relative Humidity (%)"),
         new=c("NO", "NO2", "O3", "CO", "CO2", "PM1", "PM25", "PM10", "Temperature", "RelHumidity"))
dt <- dt[, .(timestamp, NO, NO2, O3, CO, CO2, PM1, PM25, PM10, Temperature, RelHumidity)]
setorder(dt, timestamp)

# Set -9999 to NA
dt <- dt[, lapply(.SD, na_if, y=-9999)]

####### Clean up data on a per-gas basis
# The only gas that needs cleaning is the CO2 for which I'll just remove
# entirely
dt[, CO2 := NA]

# Lead MAQS data by 1 minute
dt <- dt[, lapply(.SD, lead, 1)] 
dt$timestamp <- dt$timestamp - minutes(1)
dt <- dt[1:(nrow(dt) - 1)]

# Add location column
dt[, location := "Manchester"]

# Reorder to match DB

colorder <- tbl(con, "ref") %>% colnames()
setcolorder(dt, colorder)

# Add new values
dbAppendTable(con, "ref_raw", dt)
