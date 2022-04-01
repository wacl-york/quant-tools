# collate_Reference.R
# ~~~~~~~~~~~~~~~~~~~

# Collates reference data from all sites into a single CSV
# that can be uploaded into the DB
# Data is collated from the period 2019-12-10 to 2021-06-30
# The raw data is obtained from the QUANT/Data/Reference folder on Google Drive:
# https://drive.google.com/drive/u/0/folders/18AixKgVDHn-kBhdofbTZzQBAhlISL-G_
# This needs to be downloaded with the local path set in the `ref_data_dir` variable
library(data.table)
library(lubridate)
library(tidyverse)

ref_data_dir <- "~/Documents/quant_data/Reference"

load_ref <- function() {
    # Load Aviva reference
    aviva_ref <- fread(sprintf("%s/York/Aviva/aviva_reference.csv", ref_data_dir))
    aviva_ref[, TimeBeginning := as_datetime(TimeBeginning, format="%Y-%m-%d %H:%M")]
    # Remove empty column
    aviva_ref[, V51 := NULL]
    # Clean column names
    old <- colnames(aviva_ref)
    clean <- gsub("^[0-9]+_", "", old)
    clean <- gsub("_[0-9]+", "", clean)
    clean[1] <- 'timestamp'
    setnames(aviva_ref, old, clean)
    
    # Subset to measurands of interest
    aviva_measurand_cols <- c('NO2_Scaled', 'O3_Scaled', 'NO_Scaled', 'TEMP_Scaled', 'HUM_Scaled')
    cols_to_keep <- c("timestamp", aviva_measurand_cols)
    aviva_ref <- aviva_ref[, ..cols_to_keep]
    setnames(aviva_ref, old=aviva_measurand_cols, new=gsub("_Scaled", "", aviva_measurand_cols))
    setnames(aviva_ref, old=c("TEMP", "HUM"), new=c("Temperature", "RelHumidity"))
    
    # Load Brum reference
    brum_gas <- fread(sprintf("%s/Birmingham/gas_data_BAQS_min_oct_2019_feb_2021_ratified.csv", ref_data_dir))
    brum_mcea <- fread(sprintf("%s/Birmingham/MCEA_data_BAQS_min_Jan_2020_Feb_2021.csv", ref_data_dir))
    brum_met <- fread(sprintf("%s/Birmingham/MET_BAQS_min_Jan_2020_Feb_2021.csv", ref_data_dir))
    brum_ref <- merge(brum_gas, brum_mcea, all=TRUE, by="date")
    brum_ref <- merge(brum_ref, brum_met, all=TRUE, by="date")
    brum_ref[, date := as_datetime(date, format="%d/%m/%Y %H:%M")]
    
    setnames(brum_ref,
             old=c("date", "O3_ppb", "NO2CAPS_ppb", "NO_ppb", "CO_ppm", "T", "RH"),
             new=c("timestamp", "O3", "NO2", "NO", "CO", "Temperature", "RelHumidity")
             )
    # Convert CO to ppb
    brum_ref[, CO := CO * 1000 ]
    
    # Remove flagged values
    brum_ref[ NOFlags != "1b", NO := NA]
    brum_ref[ O3Flags != "1b", O3 := NA]
    brum_ref[ NO2Flags != "1b", NO2 := NA]
    brum_ref[ CO_Flag != "1b", CO := NA]
    
    # Subset to cols of interest
    brum_ref <- brum_ref[, .(timestamp, O3, NO2, NO, CO, Temperature, RelHumidity)]
    
    manchester_ref <- fread(sprintf("%s/Manchester/Ratified/york_toJan2021.csv", ref_data_dir))
    manchester_ref[, timestamp := as_datetime(datetime, format="%d/%m/%Y %H:%M")]
    manchester_ref_met <- fread(sprintf("%s/Manchester/Ratified/york_met_toJan2021.csv", ref_data_dir))
    setnames(manchester_ref_met, old="datetime", new="timestamp")
    manchester_ref <- merge(manchester_ref, manchester_ref_met, all=TRUE, by="timestamp")
    
    # Rename measurands of interest
    setnames(manchester_ref,
             old=c("NO (ppb) perlim", "NO2 (ppb) perlim", "Ozone (ppb) perlim", "PM1 (ug/m3)", "PM2.5 (ug/m3)", "PM10 (ug/m3)", "Temperature (deg C)", "Humidity (%)"),
             new=c("NO", "NO2", "O3", "PM1", "PM2.5", "PM10", "Temperature", "RelHumidity"))
    
    # Subset to cols I'm interested in
    manchester_ref <- manchester_ref[, .(timestamp, NO, NO2, O3, PM1, PM2.5, PM10, Temperature, RelHumidity)]
    
    # London
    london_ref <- fread(sprintf("%s/London/Ratified 2020/HP_2021_YORK.csv", ref_data_dir))
    old_names <- colnames(london_ref)
    # Remove the odd column naming convention
    setnames(london_ref,
             old=old_names,
             new=gsub("@HP1$", "", old_names))
    setnames(london_ref, old="Date&Time", new="timestamp")
    setnames(london_ref, old="PM25", new="PM2.5")
    london_ref[, timestamp := as_datetime(timestamp, format="%d-%m-%y %H:%M") ]
    
    # Subset to columns of interest
    london_ref <- london_ref[, .(timestamp, NO, NO2, O3, PM10, PM2.5, PM1)]
    
    # Combine sites
    aviva_ref[, location := 'York' ]
    brum_ref[, location := 'Birmingham' ]
    manchester_ref[, location := 'Manchester' ]
    london_ref[, location := 'London' ]
    comb_ref <- rbind(aviva_ref, brum_ref, fill=TRUE)
    comb_ref <- rbind(comb_ref, manchester_ref, fill=TRUE)
    comb_ref <- rbind(comb_ref, london_ref, fill=TRUE)
    
    comb_ref
}

# Load reference
dt_ref <- load_ref()
dt_ref_long <- melt(dt_ref, id.vars=c("timestamp", "location"), value.name="reference")

# Load to one minute average minimum
dt_ref_long[, timestamp := floor_date(timestamp, "1 min")]
dt_ref_long <- dt_ref_long[, list("reference" = mean(reference, na.rm=T)), by=c("timestamp", "location", "variable")]

# Restrict to study period
dt_ref_long <- dt_ref_long[ timestamp >= as_datetime("2019-12-10") ]

# Remove missing
dt_ref_long <- dt_ref_long[ !is.na(reference) ]

fwrite(dt_ref_long, "Data/Reference.csv")

