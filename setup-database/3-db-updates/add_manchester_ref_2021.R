# add_manchester_ref_2021.R
# ~~~~~~~~~~~~~~~~~~~~~~~~~
#
# Adds the unratified reference data from the Manchester supersite for 
# the calendar year 2021 to the database. Previously only ratified data was included
#
# NB: CO2 IS NOT BEING ADDED TO THE DB
# This is because the CO2 data is very noisy and would be very hard to clean
# manually
#
# The raw unratified data must be downloaded locally from the Shared Drive
# QUANT/Data/Reference/Manchester, and it is all the files that are *not* in the 
# ratified sub-directory
# There should be 2 types of files: MAQS per month and 1 overall CO2 file
# 
# In the BOCS/Calibration/Analyses/data/Reference/Manchester/validating-2021-maqs dir
# I have run some analyses looking at how reliable this new dataset is
library(tidyverse)
library(data.table)
library(RSQLite)
library(lubridate)
library(odbc)
library(Rcpp)

sourceCpp(code="#include <Rcpp.h>
// [[Rcpp::export]]
Rcpp::NumericVector emaRcpp(Rcpp::NumericVector x, double a){
  int n = x.length();
  Rcpp::NumericVector s(n);
  s[0] = x[0];
  double prev_state;
  if (n > 1) {
    for (int i = 1; i < n; i++) {
      if (ISNAN(s[i-1])) {
        // Don't update previous state when missing!
        prev_state = prev_state;
      } else {
        prev_state = s[i-1];
      }
      s[i] = a * x[i] + (1 - a) * prev_state;
    }
  }
  return s;
}
")

DB_FN <- "~/Documents/quant_data/quant.db"
local_raw_dir <- "~/GoogleDrive/WACL/BOCS/Calibration/Analyses/data/Reference/Manchester/extract_20220217"
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

# Get 2021 reference data from Manchester that's already in the DB 
# (first 10 days until 10th Jan)
con <- dbConnect(SQLite(), DB_FN)
db_ref <- tbl(con, "ref") %>%
    filter(location == "Manchester") %>%
    collect() %>%
    mutate(timestamp = as_datetime(timestamp))

# CO2 from 2019-2020
co2_fn <- Sys.glob(file.path(local_raw_dir, "*CO2*"))
df_co2 <- readxl::read_xlsx(co2_fn)
setDT(df_co2)
# For some reason the read_xlsx function returns both the Date and Time 
# columns as datetime. Convert these to what's appropriate
df_co2[, Date := as.character(as_date(Date))]
df_co2[, Time := gsub("^.+ ", "", as.character(Time))]
df_co2[, timestamp := paste(Date, Time, sep=" ")]
df_co2[, timestamp := as_datetime(timestamp, format="%Y-%m-%d %H:%M:%S")]
setnames(df_co2, old="CO2 (ppb)", new="CO2")
df_co2 <- df_co2[, .(timestamp, CO2)]

# NB: Going to create a copy of the entire data frame to hold corrections
# since _all_ CO2 values are removed.
dt_cor <- copy(dt)

####### Clean up data on a per-gas basis
# NO:
#  - remove negative values
dt_cor[ timestamp >= "2021-06-26 23:08:00" & timestamp <= "2021-06-26 23:09:00", NO := NA ]
dt_cor[ timestamp >= "2021-06-30 19:28:00" & timestamp <= "2021-06-30 19:29:00", NO := NA ]
dt_cor[ timestamp >= "2021-07-01 17:01:00" & timestamp <= "2021-07-01 17:02:00", NO := NA]

#  - NO2:
#     - Remove Jan only
# Use these from the original dataset
dt[ timestamp >= "2021-01-02 00:14:00" & timestamp <= "2021-01-02 04:17:00", NO2 := NA ]
replacement_no2 <- db_ref %>%
    filter(timestamp >= "2021-01-02 00:13:00", timestamp <= "2021-01-02 04:16:00") %>%
    pull(NO2)
dt[ timestamp >= "2021-01-02 00:14:00" & timestamp <= "2021-01-02 04:17:00", NO2 := replacement_no2 ]

#  - O3:
#    - Remove 8th June negative bit
dt_cor[ timestamp >= "2021-06-08 16:05:00" & timestamp <= "2021-06-08 16:23:00", O3 := NA ]
#     - Remove 30th July negative
dt_cor[ timestamp >= "2021-07-30 13:16:00" & timestamp <= "2021-07-30 13:27:00", O3 := NA ]

# CO:
#   - Remove high spikes
dt_cor[ timestamp >= "2021-01-30 12:08:00" & timestamp <= "2021-01-30 12:09:00", CO := NA]
dt_cor[ timestamp >= "2021-04-08 02:39:00" & timestamp <= "2021-04-08 02:40:00", CO := NA]
dt_cor[ timestamp >= "2021-05-01 12:55:00" & timestamp <= "2021-05-01 12:56:00", CO := NA]
dt_cor[ timestamp >= "2021-09-24 12:16:00" & timestamp <= "2021-09-24 12:18:00", CO := NA]
dt_cor[ timestamp >= "2021-10-26 13:17:00" & timestamp <= "2021-10-26 13:26:00", CO := NA]
dt_cor[ timestamp >= "2021-11-19 14:41:00" & timestamp <= "2021-11-19 14:41:00", CO := NA]
#   - Remove the negative spike
dt_cor[ timestamp >= "2021-10-27 09:00:00" & timestamp <= "2021-10-27 09:15:00", CO := NA]
#   - For baseline issues on 3rd June can either drop entirely or try to model baseline
# Shift the baselines, where 'b1' is the baseline created from previous 2 weeks
b1 <- min(dt_cor[ timestamp >= "2021-05-20 14:08:00" & timestamp <= "2021-06-03 01:22:00", CO], na.rm=T)
b2 <- min(dt_cor[ timestamp >= "2021-06-03 14:08:00" & timestamp <= "2021-06-20 10:20:00", CO], na.rm=T)
b3 <- min(dt_cor[ timestamp >= "2021-06-03 01:33:00" & timestamp <= "2021-06-03 04:22:00", CO], na.rm=T)
dt_cor[ timestamp >= "2021-06-03 14:08:00" & timestamp <= "2021-06-20 10:20:00", CO := CO - b2 + b1]
dt_cor[ timestamp >= "2021-06-03 01:33:00" & timestamp <= "2021-06-03 04:22:00", CO := CO - b3 + b1]

#  - CO2:
#     - Remove all downwards spikes (could interpolate middle?)
#dt[ timestamp >= "2021-06-02 08:57:00" & timestamp <= "2021-06-02 08:59:00", CO2 := NA]
# Firstly remove as much of negative values as can, then smooth with EMA
# NB: I've given up on this, I simply won't add the CO2 to the DB as it's far too noisy
# My automated outlier detection didn't pick up quite how much noise there is
# But there's loads more days after this, and a fully automated and robust solution would
# need to be found to sort this
# My initial attempt as seen below used a combination of removing values below
# a certain threshold, and then applying a moving average filter
dt_cor[, CO2 := NA]
#dt[ timestamp >= "2021-06-02 09:55:00" & timestamp <= "2021-06-03 00:23:00", CO2 := emaRcpp(pmax(CO2, 400), 0.1)]
#dt[ timestamp >= "2021-06-03 00:00:00" & timestamp <= "2021-06-03 08:50:00", CO2 := emaRcpp(pmax(CO2, 420), 0.1)]
#dt[ timestamp >= "2021-06-03 16:47:00" & timestamp <= "2021-06-04 00:23:00", CO2 := emaRcpp(pmax(CO2, 410), 0.1)]
#dt[ timestamp >= "2021-06-04 01:14:00" & timestamp <= "2021-06-05 00:45:00", CO2 := emaRcpp(pmax(CO2, 410), 0.1)]
#dt[ timestamp >= "2021-06-05 00:46:00" & timestamp <= "2021-06-05 00:49:00", CO2 := NA ]
#dt[ as_date(timestamp) == "2021-06-05", CO2 := emaRcpp(CO2, 0.4) ]
#dt[ as_date(timestamp) == "2021-06-06", CO2 := emaRcpp(pmax(CO2, 410), 0.4) ]
#dt[ timestamp >= "2021-06-07 05:16:00" & timestamp <= "2021-06-07 05:20:00", CO2 := NA ]
#dt[ as_date(timestamp) == "2021-06-07", CO2 := emaRcpp(pmax(CO2, 405), 0.4) ]
#dt[ timestamp >= "2021-06-08 15:36:00" & timestamp <= "2021-06-08 15:40:00", CO2 := NA ]
#dt[ timestamp >= "2021-06-08 15:59:00" & timestamp <= "2021-06-08 16:31:00", CO2 := NA ]
#dt[ timestamp >= "2021-06-08 17:32:00" & timestamp <= "2021-06-08 18:01:00", CO2 := NA ]
#dt[ as_date(timestamp) == "2021-06-08", CO2 := emaRcpp(pmax(CO2, 410), 0.5) ]

#     - 2020 data
#       - Remove 18th Dec 2019
#       - Remove 18th March
#       - Remove 8th July
#       - Remove 12th August

#  - Temp/RH:
#     - Remove the 2nd May
dt_cor[ timestamp >= "2021-05-02 13:51:00" & timestamp <= "2021-05-02 13:59:00", Temperature := NA ]
dt_cor[ timestamp >= "2021-05-02 13:51:00" & timestamp <= "2021-05-02 13:59:00", RelHumidity := NA ]

# Lead MAQS data by 1 minute
dt <- dt[, lapply(.SD, lead, 1)]
dt$timestamp <- dt$timestamp - minutes(1)
dt <- dt[1:(nrow(dt) - 1)]

dt_cor <- dt_cor[, lapply(.SD, lead, 1)]
dt_cor$timestamp <- dt_cor$timestamp - minutes(1)
dt_cor <- dt_cor[1:(nrow(dt_cor) - 1)]

# Add location column
dt[, location := "Manchester"]
dt_cor[, location := "Manchester"]

# Reorder to match DB
colorder <- tbl(con, "ref") %>% colnames()
setcolorder(dt, colorder)
setcolorder(dt_cor, colorder)

# Remove values that are already in the DB
dbExecute(con, "DELETE FROM ref_raw WHERE location = 'Manchester' AND timestamp >= ?",
          params=list(as.numeric(as_datetime("2021-01-01"))))

# Add new values
dbAppendTable(con, "ref_raw", dt)
dbAppendTable(con, "ref_corrections", dt_cor)

dbDisconnect(con)
