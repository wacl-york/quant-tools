# collate_Kunak.R
# ~~~~~~~~~~~~~~~
source("~/repos/quant-tools/load_data.R")
library(tidyverse)

df_clean <- load_data("~/Documents/quant_data/Clean_wider/",
                           companies="Kunak",
                           resample="1 minute",
                           subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# Have data from 2021-06-10 to now
summary(df_clean$timestamp)

# Don't need to keep DewPoint or Pressure
# Odd NO behaviour, only really start getting spikes in winter,
# and a lot of missing CO2
df_clean %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value)) +
        geom_line() +
        theme_bw() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        labs(x="", y="Pollutant (ugm-3 ppb)", title="Kunak out of the box (24 hour average)")

df_clean <- df_clean %>%
    filter( !measurand %in% c("DewPoint", "Pressure"))

# Save!
setcolorder(df_clean, c("timestamp", "manufacturer", "device", "measurand", "value"))
df_clean <- df_clean[ !is.na(value) ]
write_csv(df_clean, "Data/Kunak.csv")
