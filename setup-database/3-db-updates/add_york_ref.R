# add_york_ref.R
# ~~~~~~~~~~~~~~
# Author: Stuart Lacy
# Date: 2022-01-24
#
# Populates the SQLite database with PM reference data from the 
# York Fishergate site.
# This data is available on AURN and has been routinely scraped
# as part of the main QUANT scraping program, but was mistakenly
# not included in the initial DB setup.

library(tidyverse)
library(lubridate)
library(reticulate)
library(odbc)
library(RSQLite)

CLEAN_DIR <- "/home/stuart/Documents/quant_data/Clean"
DB_FN <- "/home/stuart/Documents/quant_data/quant.db"

# Use Python version of load_data as is more efficient
ld <- import("load_data")

# Load all York AURN data from the time-period
york_aurn <- ld$load_data(CLEAN_DIR,
                          companies="AURN",
                          end="2021-06-30",
                          subset=NULL,
                          resample=NULL)

# Remove error codes and missing values
york_aurn <- york_aurn %>%
    rename(PM25 = `PM2.5`) %>%
    mutate(PM10 = ifelse(PM10 == -99, NA, PM10),
           PM25 = ifelse(PM25 == -99, NA, PM25)) %>%
    filter(! (is.na(PM10) & (is.na(PM25)))) %>%
    mutate(location = "York")

# Prepare to append to DB
con <- dbConnect(RSQLite::SQLite(), DB_FN)

# The table is in Wide format, so need to add columns for measurands that don't have
# and reorder
required_columns <- tbl(con, "ref_raw") %>%
    colnames()
cols_to_add <- setdiff(required_columns, colnames(york_aurn))
for (col in cols_to_add) {
    york_aurn[[col]] <- NA
}
to_insert <- york_aurn[, required_columns]

# And finally append to the main referencetable!
dbAppendTable(con, "ref_raw", to_insert)
dbDisconnect(con)

