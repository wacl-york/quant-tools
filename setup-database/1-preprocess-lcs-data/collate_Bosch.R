# collate_Bosch.R
# ~~~~~~~~~~~~~~~
source("~/repos/quant-tools/load_data.R")
library(tidyverse)

df_clean <- load_data("~/Documents/quant_data/Clean_wider/",
                           companies="Bosch",
                           resample="1 minute",
                           end="2022-02-28",
                           subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# Have data from 2021-06-16 to 2021-06-15
summary(df_clean$timestamp)

# Ah this is the one where have 3 NO2 measurements... Have to think about how to handle this
# Definitely want to drop pressure though
# Maybe I create _b and _c devices for the second and third NO2 measurements?
df_clean %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value)) +
        geom_line() +
        theme_bw() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        labs(x="", y="Pollutant (ugm-3 ppb)", title="Bosch out of the box (24 hour average)")

df_clean <- df_clean %>%
    mutate(
           device = ifelse(grepl("_1$", measurand), sprintf("%s_b", device), device),
           device = ifelse(grepl("_2$", measurand), sprintf("%s_c", device), device),
           measurand = gsub("_b$", "", measurand),
           measurand = gsub("_c$", "", measurand)
           ) %>%
    filter(measurand != "Pressure")


# Save!
setcolorder(df_clean, c("timestamp", "manufacturer", "device", "measurand", "value"))
df_clean <- df_clean[ !is.na(value) ]
write_csv(df_clean, "Data/Bosch.csv")
