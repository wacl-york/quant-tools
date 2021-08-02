# collate_Aeroqual.R
# ~~~~~~~~~~~~~~~~~~
source("~/repos/QUANT-tools/load_data.R")
library(tidyverse)

INPUT_DIR <- "~/Documents/quant_data/one-off-downloads/extracted/"

# Collates Aeroqual CSV files in the Clean format from multiple archives into 2 datasets:
#   - Out of box 
#   - First calibration products
#   - Second calibration products
# These 3 datasets will be saved in the same CSV file with a column differentiating them

########################
# Out-of-box
########################

# Firstly check to see if the backed up data from # 10 is the same as the data from the initial archive
oob_1_archive <- load_data(sprintf("%s/1-Archive/clean",
                                   INPUT_DIR),
                           companies="Aeroqual",
                           start="2019-12-10",
                           end="2020-04-22",
                           resample="1 minute",
                           subset=NULL) %>%
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device)

oob_10_archive <- load_data(sprintf("%s/10-Aeroqual/Clean",
                                   INPUT_DIR),
                           companies="Aeroqual",
                           start="2019-12-10",
                           end="2020-04-22",
                           resample="1 minute",
                           subset=NULL) %>% 
                        select(-DP) %>%
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device)

# Are they equal amongst the time period up to 15th April, when the first archive runs out?
# Nope, different number of rows.
all_equal(oob_1_archive %>% filter(as_date(timestamp) <= as_date("2020-04-15")), 
          oob_10_archive %>% filter(as_date(timestamp) <= as_date("2020-04-15"))) 

# Let's see what these measurements correspond to.
foo1 <- oob_1_archive %>% filter(as_date(timestamp) <= as_date("2020-04-15")) %>% count(device, measurand)
foo2 <- oob_10_archive %>% filter(as_date(timestamp) <= as_date("2020-04-15"))  %>% count(device, measurand)

# Ok yes they correspond to AQY872 and AQY874 missing values in the initial archive, so archive #6 did its job
# and added these back into the clean
foo1 %>%
    left_join(foo2, by=c("device", "measurand"), suffix=c("_archive", "_newcalbackup")) %>%
    filter(n_archive != n_newcalbackup)

# And all the values in the #1 archive are the same in #10, so it looks like number 10 is reliable
oob_1_archive %>%
    left_join(oob_10_archive, by=c("timestamp", "manufacturer", "device", "measurand"), suffix=c("_archive", "_newcalbackup")) %>%
    filter(value_archive != value_newcalbackup)

# Archive 10 goes up to 22nd April
max(oob_10_archive$timestamp)

# But in this remaining week we don't have any AQY875!
# Will flag this up in the document to look at later, 
oob_10_archive %>%
    filter(as_date(timestamp) >= as_date("2020-04-16")) %>%
    count(device, measurand) %>%
    print(n=Inf)

# Last data is at 2020-03-31 04:38:00
# NB: Seba later confirmed this is a known SIM card fault of this device
oob_10_archive %>%
    filter(!is.na(value), device == "AQY875") %>%
    slice_max(order_by=timestamp, n=1)

# Transform to long (measurand-value) form and add a key indicating this is oob
oob <- oob_10_archive %>% 
        mutate(dataset="out-of-box")

########################
# First calibration
########################

# The data from Dec 10th to April 23rd is in archive #12, which was manually sent over
# This data isn't in our usual format so will need a bit of pre-processing
df_12_raw <- read_csv(sprintf("%s/12-Aeroqual/ZZ-AQY manual calibration/AQY_1stCals_2019-12-10_2020-04-23.csv", INPUT_DIR))
df_12_raw <- df_12_raw %>%
    pivot_longer(-Timestamp, names_pattern="(.+)_(.+)", names_to=c("measurand", "device")) %>%
    rename(timestamp=Timestamp)
# Have data from 2019-12-10 to 2020-04-23
summary(df_12_raw$timestamp)

# Are we missing any data?
# Yes, every piece of AQY872, nearly all AQY873A, barely any AQY874, and a quarter of 875
df_12_raw %>%
    group_by(device, measurand) %>%
    summarise(n = sum(is.na(value)),
              prop = mean(is.na(value))*100) %>%
    print(n=Inf)

# Comparing the values from this manual calibration file with the out-of-box data shows that:
#   1) The uncalibrated values are the same, providing even more confidence in our out-of-box data (row 17, 18.6==18.6)
#   2) The calibrated values are definitely different (row 17 18.6 != row 21 14.7)
df_12_raw %>%
    filter(timestamp == as_datetime("2020-01-10 05:40:00")) %>%
    left_join(oob %>% select(-manufacturer, -dataset), by=c("timestamp", "measurand", "device"), suffix=c("_manualcals", "_oob")) %>%
    arrange(device, measurand) %>%
    print(n=Inf)

# What this shows below is that the only time we have observations for manual cal but not OOB is AQY872, i.e. the missingness of 873 wasn't that it was available out-of-the box 
# and now suddenly it's missing
foo <- df_12_raw %>%
    filter(!grepl("Cal", device)) %>%
    left_join(oob %>% select(-manufacturer, -dataset), by=c("timestamp", "measurand", "device"), suffix=c("_manualcals", "_oob")) %>%
    arrange(device, measurand)
foo %>% 
    filter(is.na(value_manualcals), !is.na(value_oob)) %>% 
    count(device)

# Can we obtain these missing AQY872 values?
# Potentially. The API doesn't seem to have been retrospectively changed, i.e. the first df here is one that
# I have just scraped today (2021-07-02), and it contains the same values as the OOB values that were scraped in April 2020
# as part of Archive #1
df_872_feb <- read_csv("~/repos/QUANTscraper/data/clean/Aeroqual_AQY872_2020-02-05.csv")
df_872_feb %>%
    left_join(oob_10_archive %>% filter(device == "AQY872") %>% select(-manufacturer, -device), by=c("timestamp", "measurand"))

# Now I want to see what's available in the Clean data, and the only differences here are a 0.01 difference in RH and Temp between:
# - the value scraped just now, - the OOB, - the value in Clean
# I.e. the API is giving us OOB measurements prior to April!
clean_872 <- load_data("~/Documents/quant_data/Clean", devices="AQY872", start="2020-02-05", end="2020-02-05", subset=NULL)
df_872_feb %>%
    left_join(oob_10_archive %>% filter(device == "AQY872") %>% select(-manufacturer, -device), by=c("timestamp", "measurand")) %>%
    left_join(clean_872 %>% filter(device == "AQY872") %>% select(-manufacturer, -device) %>% pivot_longer(-timestamp, names_to="measurand"), by=c("timestamp", "measurand")) %>%
    filter(value.y != value | value.x != value) %>%
    print(n=Inf)

# In fact, can we just get the first cals straight from the Clean folder, which would be the measurements but ignoring the _Cal (which are the MOMA cals)
df_clean <- load_data("~/Documents/quant_data/Clean", companies="Aeroqual", start="2019-12-10", end="2020-04-23", subset=NULL) %>%
            select(-DP, -manufacturer) %>%
            pivot_longer(-c(timestamp, device), names_to="measurand")

# Let's compare this retrospective manually applied cals to the live data in the Clean folder
comb <- df_12_raw %>%
    mutate(key=ifelse(grepl("Cal", device), "retrospective_Cal", "retrospective_OOB"),
           device=gsub("Cal", "", device)) %>%
    pivot_wider(names_from = key, values_from=value) %>%
    left_join(df_clean %>% rename(clean=value), by=c("timestamp", "device", "measurand"))

# So the clean folder has OOB for this period!
comb %>%
    filter(retrospective_OOB != clean)

# Which we can confirm by directly comparing OOB to the Clean dataset and indeed they are the same
oob %>%
    select(-dataset, -manufacturer) %>%
    left_join(df_clean, by=c("timestamp", "device", "measurand"),
              suffix=c("_oob", "_clean")) %>%
    filter(value_oob != value_clean)

# We have situations where we didn't have OOB data in the Clean data but did in the retrospective,
# because the Clean repository had archive #6 clean it up.
comb %>%
    filter(is.na(retrospective_OOB), !is.na(clean))

# And no situations where we are missing data from clean but not from the retrospective file
comb %>%
    filter(!is.na(retrospective_OOB), is.na(clean))

# And this is the same with the cals
comb %>%
    filter(is.na(retrospective_Cal), !is.na(clean))
comb %>%
    filter(!is.na(retrospective_Cal), is.na(clean))

###########
# I'm happy then that the Clean folder contains OOB up until 23nd April, then 1st Cal after (in the usual col),
# And the 2nd cal back to 22nd April in a separate field
# I.e. if I just add in the retrospective manual cal from 18th Feb - 23rd April we'll have the full dataset.
df_clean <- load_data("~/Documents/quant_data/Clean", companies="Aeroqual", start="2019-12-10", end="2021-06-30", 
                     subset=NULL) %>%
            select(-DP, -manufacturer) %>%
            pivot_longer(-c(timestamp, device), names_to="measurand")

# Dataset = OOB if < 23rd April else 1st Cal
# Then incorporate the second cals through awkward pivoting
# Then rbind in the retrospective first cals
df_clean %>%
    distinct(measurand)
df_clean <- df_clean %>%
    mutate(dataset = ifelse(as_date(timestamp) <= as_date("2020-04-23"), "OOB", "Cal_1"),
           dataset = ifelse(grepl("cal", measurand), "Cal_2", dataset),
           measurand = gsub("_cal", "", measurand),
           measurand = gsub(" cal", "", measurand))

# Now add in those retrospective first cals
df_12_clean <- df_12_raw %>%
    filter(grepl("Cal", device)) %>%
    mutate(dataset="Cal_1",
           device = gsub("Cal", "", device)) %>%
    select(timestamp, device, measurand, value, dataset)

df_clean <- df_clean %>%
                rbind(df_12_clean)

# Just check don't have duplicate measurements, and we don't
nrow(df_clean)
df_clean %>% distinct(timestamp, device, measurand, dataset) %>% nrow()

# Will save this now
df_clean <- df_clean %>%
    mutate(manufacturer = "Aeroqual") %>%
    filter(device != "AQY436A") %>%
    select(timestamp, manufacturer, device, dataset, measurand, value)


df_clean %>%
    filter(measurand %in% c("NO2", "O3", "PM2.5", "PM10")) %>%
    mutate(timestamp = floor_date(timestamp, unit="1 day")) %>%
    group_by(timestamp, device, measurand, dataset) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
        geom_line() +
        facet_grid(cols=vars(device), rows=vars(measurand), scales="free") +
        theme_bw() +
        theme(legend.position = "bottom") +
        labs(x="", y="Gas (ppb) / PM (ug/m3)", title="Aeroqual 3 datasets (24 hour average)")

df_clean %>%
    filter(dataset != "Cal_2",
           measurand %in% c("NO2", "O3", "PM2.5", "PM10"),
           !device %in% c("AQY436A", "AQY872"),
           timestamp < as_datetime("2020-04-24")) %>%
    mutate(timestamp = floor_date(timestamp, unit="1 day")) %>%
    group_by(timestamp, device, measurand, dataset) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
        geom_line() +
        facet_grid(cols=vars(device), rows=vars(measurand), scales="free") +
        scale_colour_discrete(drop=TRUE) +
        theme_bw() +
        theme(legend.position = "bottom") +
        labs(x="", y="Gas (ppb) / PM (ug/m3)", title="Aeroqual 10th Dec 2019 to 23rd April 2020 - OOB vs 1st Cal")

# Check don't have ug/m3 between 21st and 23rd April 2020
df_clean %>%
    filter(as_date(timestamp) >= as_date("2020-04-20"),
           as_date(timestamp) <= as_date("2020-04-25"),
           measurand %in% c("NO2", "O3"),
           device != "AQY875") %>%
    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
        geom_line() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        scale_colour_discrete(drop=TRUE) +
        geom_vline(xintercept=as_datetime("2020-04-21"), colour="green") +
        geom_vline(xintercept=as_datetime("2020-04-23"), colour="green") +
        theme_bw() +
        theme(legend.position = "bottom") +
        labs(x="", y="Gas (ppb)", title="Aeroqual April. Looking for unit changes")

# After applying unit changes
cal_factors <- data.table(
    measurand=c("O3", "NO2", "CO", "NO"),
    factor=c(1.9957, 
             1.9125,
             1.1642,
             1.247
             )
) %>% mutate(factor = 1/ factor)  # My conversion factors are ppb to ug/m3. Need to invert
df_clean %>%
    filter(as_date(timestamp) >= as_date("2020-04-20"),
           as_date(timestamp) <= as_date("2020-04-25"),
           measurand %in% c("NO2", "O3"),
           device != "AQY875"
           ) %>%
    left_join(cal_factors, by="measurand") %>%
    mutate(value = ifelse(as_date(timestamp) >= as_date("2020-04-21") & as_date(timestamp) <= as_date("2020-04-22"),
                          value * factor,
                          value)
           ) %>%
    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
        geom_line() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        scale_colour_discrete(drop=TRUE) +
        geom_vline(xintercept=as_datetime("2020-04-21"), colour="green") +
        geom_vline(xintercept=as_datetime("2020-04-23"), colour="green") +
        theme_bw() +
        theme(legend.position = "bottom") +
        labs(x="", y="Gas (ppb)", title="Aeroqual April 2020 after converting ug/m3 to ppb")

# Can also inspect this
# And sure enough at midnight on 23rd (i.e. 2020-04-23 00:00:00) 
# there is a step change in O3 from 71.3 to 35.4, in both OOB and Cal1
# Also in AQY874
df_clean %>%
    filter(timestamp >= as_datetime("2020-04-22 23:45:00"),
           timestamp <= as_datetime("2020-04-23 00:15:00"),
           measurand == "O3",
           device != "AQY875",
           dataset != "Cal_2") %>%
    pivot_wider(names_from=c(dataset), values_from=value) %>%
    arrange(device, timestamp) %>%
    left_join(cal_factors, by="measurand") %>%
    print(n=Inf)

# And likewise for the start of the period we see it increase by a factor of 2
df_clean %>%
    filter(timestamp >= as_datetime("2020-04-20 23:55:00"),
           timestamp <= as_datetime("2020-04-21 00:05:00"),
           measurand == "O3",
           device != "AQY875",
           dataset != "Cal_2") %>%
    pivot_wider(names_from=c(dataset), values_from=value) %>%
    arrange(device, timestamp) %>%
    left_join(cal_factors, by="measurand") %>%
    print(n=Inf)

# So I'll clean up the dataset by applying these changes and removing DewPoint
df_clean <- df_clean %>%
    filter(measurand != "DewPoint") %>%
    left_join(cal_factors, by="measurand") %>%
    mutate(value = ifelse(as_date(timestamp) >= as_date("2020-04-21") & as_date(timestamp) <= as_date("2020-04-22") & measurand %in% c("NO2", "O3"),
                          value * factor,
                          value)
           ) %>%
    select(timestamp, manufacturer, device, dataset, measurand, value)

# Remove explicit NAs again
df_clean <- df_clean %>%
                filter(!is.na(value))

write_csv(df_clean, "Data/Aeroqual.csv")
