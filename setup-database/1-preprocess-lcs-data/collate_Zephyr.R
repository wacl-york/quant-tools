# collate_Zephyr.R
# ~~~~~~~~~~~~~~~~
source("~/repos/QUANT-tools/load_data.R")
library(tidyverse)

INPUT_DIR <- "~/Documents/quant_data/one-off-downloads/extracted/"

# Collates Zephyr CSV files in the Clean format from multiple archives into 2 datasets:
#   - Out of box 
#   - First calibration products (as there isn't a second cal product for Zephyr)
# These 2 datasets will be saved in the same CSV file with a column differentiating them

########################
# Out-of-box
########################
# These should be available in archive #3, which is the backup from before the 1st cals 
# were applied. I can test this against the #1 archive
df_1 <- load_data(sprintf("%s/1-Archive/clean",
                                   INPUT_DIR),
                           companies="Zephyr",
                           resample="1 minute",
                           subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# Have data from 2019-12-10 to 2020-04-15
summary(df_1$timestamp)

# Load backup from #2
df_2 <- load_data(sprintf("%s/2-Zephyr/Zephyr_precalibration_20200608/precalibrated-data/clean",
                                   INPUT_DIR),
                           companies="Zephyr",
                           resample="1 minute",
                           subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# Have data from 2019-04-16 to 2020-06-04
summary(df_2$timestamp)

# Load Clean
df_clean <- load_data("~/Documents/quant_data/Clean/",
                      companies="Zephyr",
                      resample="1 minute",
                      subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# Have data from 2019-12-10 to 2021-06-30
summary(df_clean$timestamp)

# Clean data prior to cals (2020-06-04) should equal #1 and #2
# Ah no I'm mistaken, the cals were retrospectively applied on 2020-02-18
# Hence why the values here are different from this date onwards
df_1 %>%
    left_join(df_clean, by=c("timestamp", "manufacturer", "device", "measurand"),
              suffix=c("_archive", "_clean")) %>%
    filter(value_clean != value_archive)

# For #2 I can't correlate this to anything, although I would hope that the values
# are _different_ to what is in Clean, since it should contain the OOB measurements
# And sure enough they are different, so I have to trust that that is due to Clean
# being cals and #2 being OOB
df_2 %>%
    left_join(df_clean, by=c("timestamp", "manufacturer", "device", "measurand"),
              suffix=c("_precalbackup", "_clean")) %>%
    filter(value_clean != value_precalbackup)

###################
# CONVERT TO PBB
###################
# I'll firstly form one big dataframe
df_1[, dataset := "OOB" ]
df_2[, dataset := "OOB" ]
df_clean[, dataset := "Cal_1" ]
df_clean <- rbindlist(list(df_1, df_2, df_clean))

# Remove unneeded measurands
df_clean <- df_clean[ ! measurand %in% c("Lat", "Latitude", "Lon", "Longitude", "PressureAmb", "RelHumPCB", "TempPCB") ]
# Rearrange to place dataset before value to be consistent with other companies
setcolorder(df_clean, c("timestamp", "manufacturer", "device", "dataset", "measurand", "value"))
# I also need to convert the gas units to ppb from ug/3
cal_factors <- data.table(
    measurand=c("O3", "NO2", "CO", "NO"),
    factor=c(1.9957, 
             1.9125,
             1.1642,
             1.247
             )
) %>% mutate(factor = 1/ factor)  # My conversion factors are ppb to ug/m3. Need to invert
df_clean <- cal_factors[df_clean, on="measurand"]
df_clean[, mean(value, na.rm=T), by="measurand"]
df_clean[, value := ifelse(!is.na(factor), factor * value, value)]
df_clean[, mean(value, na.rm=T), by="measurand"]
df_clean[, factor := NULL ]

setcolorder(df_clean, c("timestamp", "manufacturer", "device", "dataset", "measurand", "value"))

# Save!
write_csv(df_clean, "Data/Zephyr.csv")

df_clean[ measurand %in% c("NO", "NO2", "O3", "PM2.5", "PM10") ] %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand, dataset) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
        geom_line() +
        facet_grid(cols=vars(device), rows=vars(measurand), scales="free") +
        theme_bw() +
        theme(legend.position="bottom") +
        labs(x="", y="Gas (ppb) / PM (ug/m3)", title="Zephyr 3 datasets (24 hour average)")

df_clean[ measurand %in% c("NO", "NO2", "O3", "PM2.5", "PM10") & as_date(timestamp) <= as_date("2020-06-05") ] %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand, dataset) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
        geom_line() +
        facet_grid(cols=vars(device), rows=vars(measurand), scales="free") +
        theme_bw() +
        theme(legend.position="bottom") +
        labs(x="", y="Gas (ppb) / PM (ug/m3)", title="Zephyr 3 datasets (24 hour average) before first cals applied on 4th June")
