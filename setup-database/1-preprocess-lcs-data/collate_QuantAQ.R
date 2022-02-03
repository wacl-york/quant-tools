# collate_QuantAQ.R
# ~~~~~~~~~~~~~~~~~
source("~/repos/quant-tools/load_data.R")
library(tidyverse)
library(jsonlite)

INPUT_DIR <- "~/Documents/quant_data/one-off-downloads/extracted/"

# Collates QuantAQ CSV files in the Clean format from multiple archives into 3 datasets:
#   - Out of box
#   - First calibration products
#   - Second calibration products
# These 3 datasets will be saved in the same CSV file with a column differentiating them

our_devices <- c("Ari063", "Ari078", "Ari086", "Ari093")
measurands_to_keep <- c("CO", "CO2", "NO", "NO2", "O3", "PM1", "PM2.5", "PM10",
                        "RelHumidity", "Temperature")

########################
# Out-of-box
########################
# Firstly check what the difference between the January resent and the original archive is
# Load #1
df_1 <- load_data(sprintf("%s/1-Archive/clean",
                                   INPUT_DIR),
                           companies="QuantAQ",
                           resample="1 minute",
                           subset=measurands_to_keep
                        ) %>% 
                        filter(device %in% our_devices) %>%
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# data from 10th Dec to 15th March
summary(df_1$timestamp)

# Load #5
df_5 <- load_data(sprintf("%s/5-QuantAQ/data/clean",
                                   INPUT_DIR),
                           companies="QuantAQ",
                           resample="1 minute",
                           subset=measurands_to_keep
                        ) %>%
                        filter(device %in% our_devices) %>%
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# data from 19th Dec to 11th March
summary(df_5$timestamp)

# This shows that the archived data is error prone and has very different values, 
# hence why we should trust the values resent in March instead
df_5 %>%
    mutate(dataset="March resent") %>%
    rbind(df_1 %>% mutate(dataset="Archive")) %>%
    filter(measurand %in% c("NO2", "O3", "NO", "CO", "CO2", "PM1", "PM2.5", "PM10")) %>%
    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
        geom_line() +
        theme_bw() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="QuantAQ out of the box (Dec 2019 to April 2020)")

# These 2, along with #4 (backup of Clean from before first cals were run)
# should all equal OOB
# I'll now look at the backup of 4.
df_4 <- load_data(sprintf("%s/4-QuantAQ/QuantAQ_precalibrations_20200805/precalibrated_data/clean",
                                   INPUT_DIR),
                           companies="QuantAQ",
                           resample="1 minute",
                           subset=measurands_to_keep
                        ) %>%
                        filter(device %in% our_devices) %>%
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()

# Have data from 2019-12-19 to 2020-07-23
summary(df_4$timestamp)

df_4 %>%
    filter(measurand %in% c("NO2", "O3", "NO", "CO", "CO2", "PM1", "PM2.5", "PM10")) %>%
    ggplot(aes(x=timestamp, y=value)) +
        geom_line() +
        theme_bw() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="QuantAQ out of the box (Dec 2019 to June 2020)")

# Zooming in to < 500ppb and there are still errors that should have been cleaned up before.
# Whose responsibilities are these?
# Looks to me like the device has been restarted
# Also something dodgy going on with Ari086 and NO until mid January...
df_4 %>%
    filter(measurand %in% c("NO2", "O3", "NO", "CO2"),
           abs(value) < 500) %>%
    ggplot(aes(x=timestamp, y=value)) +
        geom_line() +
        theme_bw() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="QuantAQ out of the box (Dec 2019 to June 2020). Removed erroneous gas > 500ppb")

# Can keep df_4 as OOB then
# But need to save as don't have enough memory to then load the first cals!
rm(list=c("df_1", "df_5"))

df_4[, dataset := "OOB" ]
setcolorder(df_4, c("timestamp", "manufacturer", "device", "dataset", "measurand", "value"))

# Will assume missigness implicitly by absence of measurement
df_4 <- df_4[ !is.na(value)]

# Save!
write_csv(df_4, "Data/QuantAQ_OOB.csv")

##############################
# First cal product
##############################
# This should be stored in the Clean backup from #11, when the second cal product
# was applied
df_11 <- load_data(sprintf("%s/11-QuantAQ/googledrive-backup/Clean",
                                   INPUT_DIR),
                           companies="QuantAQ",
                           resample="1 minute",
                           subset=measurands_to_keep
                        ) %>% 
                        filter(device %in% our_devices) %>%
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# Have data from 2019-12-20 to 2021-04-04
summary(df_11$timestamp)

# NB: The PM 1st cals were still on the server until 12th April, so I can get
# another week of PM 1st cals from the clean folder, which I'll do now actually.
df_pm_april2021 <- load_data("~/Documents/quant_data/Clean/",
                           companies="QuantAQ",
                           resample="1 minute",
                           subset=c("PM1", "PM2.5", "PM10"),
                           start="2021-04-05",
                           end="2021-04-11"
                        ) %>% 
                        filter(device %in% our_devices) %>%
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
summary(df_pm_april2021$timestamp)

df_11 <- rbindlist(list(df_11, df_pm_april2021))

df_11 %>%
    filter(device != "Ari078") %>%
    filter(measurand %in% c("NO2", "O3", "NO", "CO", "CO2", "PM1", "PM2.5", "PM10")) %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value)) +
        geom_line() +
        theme_bw() +
        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="QuantAQ 1st cal (24 hour average)")


df_11 %>%
    filter(device == "Ari078") %>%
    filter(measurand %in% c("NO2", "O3", "NO", "CO", "CO2", "PM1", "PM2.5", "PM10")) %>%
    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
    group_by(timestamp, device, measurand) %>%
    summarise(value = mean(value, na.rm=T)) %>%
    ggplot(aes(x=timestamp, y=value)) +
        geom_line() +
        theme_bw() +
        facet_wrap(~measurand, scales="free") +
        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="Ari078 1st cal (24 hour average)")

df_11[, dataset := "Cal_1" ]

setcolorder(df_11, c("timestamp", "manufacturer", "device", "dataset", "measurand", "value"))

# NB: HAVE REMOVED PM FROM 1st Cal DATA FOLLOWING DISCUSSION WITH SEBA ON 2021-08-04
# Seba was under the impression that AriSense hadn't calibrated PM as the idea was it is up to us
# to do it, so we won't be using the PM from the first cals
df_11 <- df_11[ !grepl("PM", measurand) ]

# Save!
df_11 <- df_11[ !is.na(value)]
write_csv(df_11, "Data/QuantAQ_Cal1.csv")

##############################
# Second cal product
##############################
# This should the Clean folder.
# Remove PM from between 2020-04-05 and 2020-04-12
df_clean <- load_data("~/Documents/quant_data/Clean",
                           companies="QuantAQ",
                           resample="1 minute",
                           subset=measurands_to_keep
                        ) %>% 
                        filter(device %in% our_devices) %>%
                        pivot_longer(-c(timestamp, manufacturer, device), names_to="measurand") %>%
                        arrange(timestamp, device) %>%
                        setDT()
# Have data from 2019-12-20 to 2021-07-01
summary(df_clean$timestamp)
df_clean[, dataset := "Cal_2" ]
setcolorder(df_clean, c("timestamp", "manufacturer", "device", "dataset", "measurand", "value"))

# Save!
df_clean <- df_clean[ !is.na(value)]
write_csv(df_clean, "Data/QuantAQ_Cal2.csv")

##################################
# Compare the 3 datasets
##################################
#
#df_4 %>%
#    filter(measurand %in% c("NO2", "O3", "NO", "CO", "CO2", "PM1", "PM2.5", "PM10"),
#           as_date(timestamp) >= as_date("2020-03-10"),
#           as_date(timestamp) <= as_date("2020-03-24")
#                   ) %>%
#    ggplot(aes(x=timestamp, y=value)) +
#        geom_line() +
#        theme_bw() +
#        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
#        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="QuantAQ")
#
#fns <- list("OOB"="Data/QuantAQ_OOB.csv",
#            "Cal1"="Data/QuantAQ_Cal1.csv",
#            "Cal2"="Data/QuantAQ_Cal2.csv")
#foo <- rbindlist(lapply(fns, function(fn) {
#    df <- fread(fn)
#    df[, timestamp := as_datetime(timestamp) ]
#    df <- df[ as_date(timestamp) >= as_date("2020-03-10") & as_date(timestamp) <= as_date("2020-03-24") ]
#    df
#}))
#
## Looking at that period of missingness and there's still some issues, although
## maybe the first cal has removed that spike after it comes back online
## But OOB seems to be the worst
#foo %>%
#    filter(measurand %in% c("NO2", "O3", "NO", "CO", "CO2", "PM1", "PM2.5", "PM10")) %>%
#    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
#        geom_line() +
#        theme_bw() +
#        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
#        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="QuantAQ resetting March 2020")
#
## Looking at the 2nd cal and it actually seems to suffer from the same problem whereas the
## first cal didn't!
## We'd deff want to take this into consideration with our analysis
#foo %>%
#    filter(measurand %in% c("NO2", "O3", "NO", "CO", "CO2", "PM1", "PM2.5", "PM10")) %>%
#    filter(dataset != "OOB") %>%
#    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
#        geom_line() +
#        theme_bw() +
#        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
#        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="QuantAQ resetting March 2020")
#
#foo <- rbindlist(lapply(fns, function(fn) {
#    df <- fread(fn)
#    df[, timestamp := as_datetime(timestamp) ]
#    df
#}))
#
#foo %>%
#    filter(measurand %in% c("NO2", "O3", "NO", "CO", "CO2", "PM1", "PM2.5", "PM10")) %>%
#    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
#    group_by(timestamp, device, dataset, measurand) %>%
#    summarise(value = mean(value, na.rm=T)) %>%
#    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
#        geom_line() +
#        theme_bw() +
#        theme(legend.position="bottom") +
#        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
#        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="QuantAQ 3 dataset comparison (24 hour average)")
#
#foo %>%
#    filter(measurand %in% c("NO2", "O3", "NO", "CO", "CO2", "PM1", "PM2.5", "PM10"),
#           abs(value) < 500,
#           value > -10) %>%
#    mutate(timestamp = floor_date(timestamp, "1 day")) %>%
#    group_by(timestamp, device, dataset, measurand) %>%
#    summarise(value = mean(value, na.rm=T)) %>%
#    ggplot(aes(x=timestamp, y=value, colour=dataset)) +
#        geom_line() +
#        theme_bw() +
#        theme(legend.position="bottom") +
#        facet_grid(rows=vars(measurand), cols=vars(device), scales="free") +
#        labs(x="", y="Pollutant (ppb gas / ugm-3 ppb)", title="QuantAQ 3 dataset comparison (24 hour average, outliers removed)")
#
#
#######################################
## Identifying outliers
#######################################
## I'll identify outlier as a gas with value < -1000 or > 1e4 (not CO)
#
## I'll obtain the dates of outliers, then manually look in the raw files to see if a) these values are flagged
#
########### OOB
#
## Load oob from csv
#oob <- read_csv("Data/QuantAQ_OOB.csv")
#gases <- c("CO", "CO2", "NO", "NO2", "O3")
#oob_outliers <- oob %>%
#    filter(measurand %in% gases,
#           value < -1e3 | value > 1e4) 
#oob_dates <- oob_outliers %>%
#    distinct(device, date=as_date(timestamp)) %>%
#    mutate(fn = sprintf("QuantAQ_%s_%s.json", device, date))
#oob_dates
#
## These are evenly distributed amongst the 4 main gases
#oob_outliers %>%
#    count(measurand)
#
## Ok. Only 1 of these dates has a non-zero flag, let's take a look at it.
#oob_raw_folder <- sprintf("%s/4-QuantAQ/QuantAQ_precalibrations_20200805/precalibrated_data/raw",
#                          INPUT_DIR)
#for (i in 1:nrow(oob_dates)) {
#    this_fn <- oob_dates$fn[i]
#    raw <- fromJSON(sprintf("%s/%s", oob_raw_folder, this_fn))$raw
#    cat(sprintf("File: %s\tFlags = 0: %d\tFlags != 0: %d\n", this_fn, sum(raw$flag == 0), sum(raw$flag != 0)))
#}
#
#fn_with_flags <- "QuantAQ_Ari063_2020-04-07.json"
#raw <- fromJSON(sprintf("%s/%s", oob_raw_folder, fn_with_flags))$raw
#final <- fromJSON(sprintf("%s/%s", oob_raw_folder, fn_with_flags))$final
#
## Raw data looks ok. Reasonable voltages.
#raw[ raw$flag != 0, ]
## And the converted concentrations also look good
## Unsure what this flag is for, but it isn't the cause of these dodgy results
#final[ raw$flag != 0, ]
#
## Finally, these flag occurred at 08:50 and 08:52, while the outliers we saw occurred between 09:07 - 09:12
#oob_outliers %>% filter(as_date(timestamp) == "2020-04-07")
#
## And where we have these crazy high values seen here with massive negative NO, NO2, O3 in the final dataset...
#final %>% 
#    filter(as_datetime(timestamp) >= as_datetime("2020-04-07 09:07:00"), 
#           as_datetime(timestamp) < as_datetime("2020-04-07 09:12:00"))
## The flags are 0!
#raw %>% 
#    filter(as_datetime(timestamp) >= as_datetime("2020-04-07 09:07:00"), 
#           as_datetime(timestamp) < as_datetime("2020-04-07 09:12:00"))
#
########### 1st Cal
#
#cal_1 <- read_csv("Data/QuantAQ_Cal1.csv")
#gases <- c("CO", "CO2", "NO", "NO2", "O3")
#cal1_outliers <- cal_1 %>%
#    filter(measurand %in% gases,
#           value < -1e3 | value > 1e4) 
## Have a few more outliers this time!
## Interestingly, looks like the outliers from the OOB dataset (March-May) have now been removed,
## and there are no outliers in the retrospective data (would have been added in #4 up to 2020-07-23)
## This was also the case with OOB, where the data they sent over to us had no outliers - it was all in the
## data from the API
#cal1_outliers
#cal1_dates <- cal1_outliers %>%
#    distinct(device, date=as_date(timestamp)) %>%
#    mutate(fn = sprintf("QuantAQ_%s_%s.json", device, date))
#cal1_dates
#
## These are mostly CO, but with a few NO and NO2 as well
#cal1_outliers %>%
#    count(measurand)
#
## I'll just see if the humongous values from the OOB dates have been removed
## Yep, nothing dodgy here
## In fact, it looks just like they've been removed
#cal_1 %>%
#    filter(timestamp >= as_datetime("2020-04-07 09:07:00"),
#           timestamp <= as_datetime("2020-04-07 09:12:00"),
#           measurand %in% c("NO", "NO2", "O3", "CO", "CO2"),
#           device == "Ari063")
#
## Ok. Only 3 of these dates has a non-zero flag, let's take a look them
#cal1_raw_folder <- sprintf("%s/11-QuantAQ/googledrive-backup/Raw",
#                          INPUT_DIR)
#for (i in 1:nrow(cal1_dates)) {
#    this_fn <- cal1_dates$fn[i]
#    raw <- fromJSON(sprintf("%s/%s", cal1_raw_folder, this_fn))$raw
#    cat(sprintf("File: %s\tFlags = 0: %d\tFlags != 0: %d\n", this_fn, sum(raw$flag == 0), sum(raw$flag != 0)))
#}
#
## Let's look at these 3 files in turn
#
## The first file *only* has non-zero flags!
#fn_with_flags <- "QuantAQ_Ari086_2020-10-29.json"
## This was flagged due to a CO value having negative -2000, but that is it
#cal1_outliers %>%
#    filter(device == "Ari086", as_date(timestamp) == "2020-10-29")
## And looking at it, only CO and CO2 seem to be affected!
## O3, NO2, and NO all seem fine.
#cal_1 %>%
#    filter(device == "Ari086", as_date(timestamp) == "2020-10-29") %>%
#    group_by(measurand) %>%
#    summarise(max(value, na.rm=T),
#              min(value, na.rm=T))
## And sure enough, we have 0 clean CO2 values for this date, so maybe the flags were
## just there to tell us that every row had dirty CO2
#cal_1 %>%
#    filter(device == "Ari086", as_date(timestamp) == "2020-10-29") %>%
#    group_by(measurand) %>%
#    summarise(sum(!is.na(value) & !is.infinite(value)))
#
#raw <- fromJSON(sprintf("%s/%s", cal1_raw_folder, fn_with_flags))$raw
#final <- fromJSON(sprintf("%s/%s", cal1_raw_folder, fn_with_flags))$final
## The flags are all the same - 256 - which I assume is dodgy CO2
#table(raw$flag)
#
## The CO voltages look ok, but the CO2 raw value is 0, hence the flag of 256
#raw %>%
#    filter(as_datetime(timestamp) >= as_datetime("2020-10-29 04:52:00"),
#           as_datetime(timestamp) < as_datetime("2020-10-29 04:53:00"))
## The final value looks reasonable for O3, NO, and NO2 but is definitely too low for CO!
#final %>%
#    filter(as_datetime(timestamp) >= as_datetime("2020-10-29 04:52:00"),
#           as_datetime(timestamp) < as_datetime("2020-10-29 04:53:00"))
#
## So that first file then had all flags cos of CO2, but otherwise the dodgy CO value looked fine
#
## Let's look at the second file, which also had a lot of flags
#fn_with_flags <- "QuantAQ_Ari086_2020-11-04.json"
## This file was flagged due to negative values in NO and NO2 exclusively
## This occurred at 3 different time periods between 13:19 and 14:43
#cal1_outliers %>%
#    filter(device == "Ari086", as_date(timestamp) == "2020-11-04")
## And looking at it, this is mostly an issue with NO and NO2, although it does look like CO also had a bit of an issue
## O3, NO2, and NO all seem fine.
#cal_1 %>%
#    filter(device == "Ari086", as_date(timestamp) == "2020-11-04") %>%
#    group_by(measurand) %>%
#    summarise(max(value, na.rm=T),
#              min(value, na.rm=T))
## And this occurred after those other issues
#cal_1 %>%
#    filter(device == "Ari086", as_date(timestamp) == "2020-11-04",
#           measurand == "CO", value < -500)
#
## And it looks like we're lacking nearly half of CO2 values, which should again account
## for the flags
#cal_1 %>%
#    filter(device == "Ari086", as_date(timestamp) == "2020-11-04") %>%
#    group_by(measurand) %>%
#    summarise(sum(!is.na(value) & !is.infinite(value)))
#
#raw <- fromJSON(sprintf("%s/%s", cal1_raw_folder, fn_with_flags))$raw
#final <- fromJSON(sprintf("%s/%s", cal1_raw_folder, fn_with_flags))$final
## Again, the flags are 256, which I reckon is an error with the CO2 measurement
#table(raw$flag)
#
## The voltages look reasonable except for the NO_we suddenly plummeting
## NO2 look reasonable however, so not sure why these measurements are so low
## No flag as CO2 is fine
#raw %>%
#    filter(as_datetime(timestamp) >= as_datetime("2020-11-04 13:19:00"),
#           as_datetime(timestamp) < as_datetime("2020-11-04 13:25:00"))
## Yep no idea why NO2 is so bad when the voltages looked fine.
#final %>%
#    filter(as_datetime(timestamp) >= as_datetime("2020-11-04 13:19:00"),
#           as_datetime(timestamp) < as_datetime("2020-11-04 13:25:00"))
#
## Once again, the flags worked and the NO and NO2 values were simply not removed
## OR their model simply doesn't work well
## The NO2 histogram shows that most values are located around 0, so this NO2 value should have been fine
#hist(raw$no2_diff)
## NO values should also be around 0, so it looks like the NO input is used in the NO2 model, which is what
## caused the problem
#hist(raw$no_diff)
#
## Again, it looks like a problem with the sensor that wasn't flagged up, as the flags are definitely being removed
#
## The third file then!
#fn_with_flags <- "QuantAQ_Ari063_2021-02-04.json"
## This file was flagged due to to one spike in CO at 11:23
#cal1_outliers %>%
#    filter(device == "Ari063", as_date(timestamp) == "2021-02-04")
## And there is also an error with NO being overly negative
#cal_1 %>%
#    filter(device == "Ari063", as_date(timestamp) == "2021-02-04") %>%
#    group_by(measurand) %>%
#    summarise(max(value, na.rm=T),
#              min(value, na.rm=T))
## Which occurred at the same time actually!
#cal_1 %>%
#    filter(device == "Ari063", as_date(timestamp) == "2021-02-04",
#           measurand == "NO", value < -500)
#
## And here it looks like we have nearly every value we'd expect so I'm interested to see what 
## caused the 6 flags
#cal_1 %>%
#    filter(device == "Ari063", as_date(timestamp) == "2021-02-04") %>%
#    group_by(measurand) %>%
#    summarise(sum(!is.na(value) & !is.infinite(value)))
#
#raw <- fromJSON(sprintf("%s/%s", cal1_raw_folder, fn_with_flags))$raw
#final <- fromJSON(sprintf("%s/%s", cal1_raw_folder, fn_with_flags))$final
## The 6 flags are 128, which I haven't seen before
#table(raw$flag)
#
## Ah wow yeah that's quite a high co_diff of 3280
## Again, no flags though
#raw %>%
#    filter(as_datetime(timestamp) >= as_datetime("2021-02-04 11:23:00"),
#           as_datetime(timestamp) < as_datetime("2021-02-04 11:24:00"))
## Especially as most are around 0!
#hist(raw$co_diff)
#
## So CO must also be used in the NO model, good to know
#
################## 2nd cals
#cal_2 <- read_csv("Data/QuantAQ_Cal2.csv")
#gases <- c("CO", "CO2", "NO", "NO2", "O3")
#cal2_outliers <- cal_2 %>%
#    filter(measurand %in% gases,
#           value < -1e3 | value > 1e4) 
## There are a lot of outliers!
## Interestingly it looks a lot to me like some kind of overflow issue with CO = 13529
#cal2_outliers %>%
#    print(n=Inf)
## Indeed nearly all of these outliers are CO!
#cal2_outliers %>%
#    count(device, measurand)
#
## Let's look at O3, these all occurred on the same day in Feb 2020,
## Which we haven't seen before
#cal2_outliers %>%
#    filter(measurand == "O3")
#
## There doesn't seem to be a pattern to the dates - they are all over the place
## Notably they also include the backfilled dates!
#cal2_dates <- cal2_outliers %>%
#    distinct(device, date=as_date(timestamp)) %>%
#    mutate(fn = sprintf("QuantAQ_%s_%s.json", device, date))
#unique(cal2_dates$date)
#
## I don't have the individual days saved in JSON, but I can load a full raw dataset at once
#cal2_raw_folder <- sprintf("%s/11-QuantAQ/backfilled-data/raw",
#                          INPUT_DIR)
#ari93_raw <- fread("../one-off-downloads/extracted/11-QuantAQ/backfilled-data/raw/SN000-093.csv")
## Here are the 5 rows that have ddogy O3 values, and sure enough the ox_diff column is weird, 
## but there's no flag and it's not an obvious error
#ari93_raw[ as_datetime(timestamp) >= as_datetime("2020-02-21 19:43:00") & as_datetime(timestamp) < as_datetime("2020-02-21 19:48:00")]
## And flags are definitely used!
#table(ari93_raw$flag)
#
## Are there are any CO outliers we can look at from this device?
## Yep lots
#cal2_outliers %>%
#    filter(device == "Ari093") %>%
#    print(n=20)
#
## Let's look at this 10 min stretch where CO had outrageous values
## Sure enough the CO_diff value is massive at 3,300!
## Which seems driven by co_ae = 0 and co_we = supply voltage!
## However, this isn't flagged, so is it our responsibility?
#ari93_raw[ as_datetime(timestamp) >= as_datetime("2020-02-07 17:41:00") & as_datetime(timestamp) < as_datetime("2020-02-07 17:51:00")]
#
## Especially because the average CO diff is 0!
#hist(ari93_raw$co_diff)
#
## Hmmm is it because the device_state is equal to CALIBRATION?!
#table(ari93_raw$device_state)
#
## Are all our errors from when the device state is calibrating?
## Nope, a lot are active too
#ari93_raw[, timestamp_min := floor_date(timestamp, "1 min")]
#cal2_outliers %>%
#    filter(device == "Ari093") %>%
#    left_join(ari93_raw[, .(timestamp_min, device_state)], by=c("timestamp"="timestamp_min")) %>%
#    count(device_state)
#
## Yep 12 examples where co_ae is 0 and device state is active, and sure enough no flags
#ari93_raw[ co_ae == 0 & device_state == "ACTIVE"]
#
##########################
## Summary
#########################
## Plenty of examples where there are errors due to issues with the voltages BUT the flags aren't working
## The flags are definitely being removed from the final dataset, as evidenced by the dodgy CO being removed
#
## I'd like to compare the outliers across all 3 datasets to see if they exist in each or whether they've been removed, or even if
## they've only been modified
#
## Combine all outlier tables together
#all_outliers <- oob_outliers %>%
#    rbind(cal1_outliers) %>%
#    rbind(cal2_outliers) %>%
#    rename(dataset_outlier=dataset, value_outlier=value) %>%
#    setDT()
#
## Are there any outliers that occur in multiple datasets?
## Yes quite a lot of ones where there was an outlier in OOB and Cal2, but not the first cal!
#all_outliers %>%
#   group_by(timestamp, manufacturer, device, measurand) %>%
#   filter(n() > 1) %>%
#   arrange(timestamp, manufacturer, device, measurand, dataset_outlier)
#
## Looking at the first of these and ah, it simply doesn't exist.
## We don't have first cals for this time point
## I wonder if they were simply removed?
#cal_1 %>%
#    filter(timestamp == as_datetime("2020-03-17 11:23:00"), device == "Ari063", measurand == "CO")
## Yep we have data for this day from CO, but only from 14h onwards
#cal_1 %>%
#    filter(as_date(timestamp) == as_date("2020-03-17"), device == "Ari063", measurand == "CO") %>%
#    arrange(timestamp)
## And no data the previous day
#cal_1 %>%
#    filter(as_date(timestamp) == as_date("2020-03-16"), device == "Ari063", measurand == "CO") %>%
#    arrange(timestamp)
#
## Looking at how often these outliers overlap, and 30 of them are when have 
## outliers in OOB and Cal2, and 54 are when an outlier is kept in from Cal1 to Cal2
## So Cal1 removed all OOB errors, good to know
#all_outliers %>%
#   group_by(timestamp, manufacturer, device, measurand) %>%
#   filter(n() > 1) %>%
#   ungroup() %>%
#   group_by(timestamp, manufacturer, device, measurand) %>%
#   count(dataset_outlier) %>%
#    ungroup() %>%
#    pivot_wider(names_from=dataset_outlier, values_from=n) %>%
#    count(OOB, Cal_1, Cal_2)
#
## Then form table
#gases_wide <- gases_wide[all_outliers, on=c("timestamp", "manufacturer", "device", "measurand")]
#
## Let's look to see where the outliers have changed
#gases_wide[ value != value_outlier]
