# collate_PurpleAir.R
# ~~~~~~~~~~~~~~~~~~~
source("~/repos/QUANT-tools/load_data.R")
library(tidyverse)

INPUT_DIR <- "~/Documents/quant_data/one-off-downloads/extracted/"

# Collates PurpleAir CSV files in the Clean format from multiple archives into 3 datasets:
#   - Out of box
#   - First calibration products
#   - Second calibration products
# Well it would do, but PurpleAir don't calibrate their products with reference data,
# so we only have a single dataset, which is out-of-the-box.
df_clean <- load_data("~/Documents/quant_data/Clean/",
                           companies="PurpleAir",
                           resample="1 minute",
                           subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# Have data from 2019-12-10 to 2021-06-15
summary(df_clean$timestamp)

fahrenheit_to_celsius <- function(degF) (degF - 32) / 1.8

# Convert Temp to C and indicate second PM sensors in device ID rather than measurand
df_clean <- df_clean %>%
    mutate(value = ifelse(measurand == "Temperature_F", fahrenheit_to_celsius(value), value),
           measurand = gsub("Temperature_F", "Temperature", measurand),
           device = ifelse(grepl("_b$", measurand), sprintf("%s_b", device), device),
           measurand = gsub("_b$", "", measurand)
           ) %>%
    filter(measurand %in% c("PM1", "PM2.5", "PM10", "Temperature", "RelHumidity"))
           

df_clean %>%
    filter(grepl("PM", measurand)) %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value)) +
        geom_line() +
        theme_bw() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        labs(x="", y="Pollutant (ugm-3 ppb)", title="PurpleAir out of the box (24 hour average)")

df_clean %>%
    filter(grepl("PM", measurand),
           device != "PA3_b") %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value)) +
        geom_line() +
        theme_bw() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        labs(x="", y="Pollutant (ugm-3 ppb)", title="PurpleAir out of the box (24 hour average)")

# Save!
df_clean[, dataset := "OOB" ]
setcolorder(df_clean, c("timestamp", "manufacturer", "device", "dataset", "measurand", "value"))
write_csv(df_clean, "Data/PurpleAir_OOB.csv")
