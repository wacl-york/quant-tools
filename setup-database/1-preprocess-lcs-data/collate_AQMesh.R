# collate_AQMesh.R
# ~~~~~~~~~~~~~~~~
source("~/repos/QUANT-tools/load_data.R")
library(tidyverse)

INPUT_DIR <- "~/Documents/quant_data/one-off-downloads/extracted/"

# Collates AQMesh CSV files in the Clean format from multiple archives into 2 datasets:
#   - Out of box 
#   - First calibration products
#   - Second calibration products
# These 3 datasets will be saved in the same CSV file with a column differentiating them

########################
# Out-of-box
########################
# These should be available in archive #3, which is the backup from before the 1st cals 
# were applied. I can test this against the #1 archive
df_1 <- load_data(sprintf("%s/1-Archive/clean",
                                   INPUT_DIR),
                           companies="AQMesh",
                           resample="1 minute",
                           subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# Have data from 2019-12-10 to 2020-04-15
summary(df_1$timestamp)

# Seem to have decent amounts of data
df_1 %>%
    count(device, measurand) %>%
    print(n=Inf)

df_3 <- load_data(sprintf("%s/3-AQMesh/precalibrated_data/clean",
                                   INPUT_DIR),
                           companies="AQMesh",
                           resample="1 minute",
                           subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()

# Have data from 2020-04-16 to 2020-06-03 
# In the README I mentioned I didn't want to have overlapping time-periods
summary(df_3$timestamp)

# Seem to have decent amounts of data
df_3 %>%
    count(device, measurand) %>%
    print(n=Inf)

# Can combine into a single dataset
oob <- rbindlist(list(df_1, df_3))

########################
# First cals (rebased)
########################
# The rebased cals (1st product) were applied in March 2021
# I'm going to ignore the non-rebased values here at the moment
# The rebased data should be in the main Clean folder, having being placed
# there following the retrospective rescrape of the rebased in #9, then I assume
# it was just rebased data coming through the API.
df_clean <- load_data("~/Documents/quant_data/Clean/",
                      companies="AQMesh",
                      resample="1 minute",
                      subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                     setDT()
summary(df_clean$timestamp)


# I'll combine it with OOB and the first cal (before rebased) to compare
df_9 <- load_data(sprintf("%s/9-AQMesh/Clean",
                                   INPUT_DIR),
                           companies="AQMesh",
                           resample="1 minute",
                           subset=NULL) %>% 
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# We have the cals prior to rebasing up until 2021-02-24, which is what I had expected
summary(df_9$timestamp)

df_clean[, dataset := "Cals_1_Rebased" ]
oob[, dataset := "OOB" ]
df_9[, dataset := "Cals_1" ]

df_clean <- rbindlist(list(df_clean, oob, df_9))
# Drop the 2 hired devices
df_clean <- df_clean[ device != "AQM173" ]
df_clean <- df_clean[ device != "AQM801" ]
df_clean <- df_clean %>%
                select(timestamp, manufacturer, device, dataset, measurand, value)

# Save!
write_csv(df_clean, "Data/AQMesh.csv")

df_clean[ measurand %in% c("NO", "O3", "PM2.5", "PM10") ] %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand, dataset) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
        geom_line() +
        facet_grid(cols=vars(device), rows=vars(measurand), scales="free") +
        theme_bw() +
        theme(legend.position="bottom") +
        labs(x="", y="Gas (ppb) / PM (ug/m3)", title="AQMesh 3 datasets (24 hour average)")

df_clean[ measurand %in% c("PM2.5", "PM10") & timestamp < as_datetime("2020-06-04") & value < 100 ] %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand, dataset) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
        geom_line() +
        facet_grid(cols=vars(device), rows=vars(measurand), scales="free") +
        theme_bw() +
        theme(legend.position="bottom") +
        labs(x="", y="PM (ug/m3)", title="AQMesh PM 3 datasets (24 hour average)")
    
