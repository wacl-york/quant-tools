# Insert new LCS measurements into the DB
#
# update_chronologically() looks for the latest date with data in the DB
#  for each instrument and attempts to insert any more recent data from the
#  Google Drive folder in bulk.
#
# update_gaps() finds any days with no data available for every
#  instrument and attempts to insert that data from the Google Drive
#  folder if available.
library(tidyverse)
library(lubridate)
library(odbc)
source("load_data.R")

update_chronologically <- function(con) {
    # Get all LCS companies with the latest date of data and current cal version
    companies <- tbl(con, "lcscompany") |>
        inner_join(tbl(con, "lcsinstrument"), by="company") |>
        collect() |>
        ungroup() |>
        filter(instrument != "AQY875",  # AQY875 hasnt been running since 2021
               instrument != "IMB1",    # IMB1 hasn't working since March 2022
               company != "RLS")        # RLS hasn't had any data
    
    latest_lcs <- tbl(con, "lcsinstrument") |>
        left_join(tbl(con, "measurement"), by="instrument") |>
        group_by(instrument) |>
        summarise(latest_date = as_date(max(time))) |>
        collect() |>
        ungroup()
    
    latest_lcs <- latest_lcs |>
        inner_join(companies, by="instrument")
    
    local_folder <- tibble(study=c("QUANT", "Wider Participation"), folder=c("~/Documents/quant_data/Clean/", "~/Documents/quant_data/Clean_wider/"))
    latest_lcs <- latest_lcs |> inner_join(local_folder, by="study")
    
    measurands <- tbl(con, "measurand") |>
                    collect() |>
                    pull(measurand)
    
    for (i in 1:nrow(latest_lcs)) {
        cat(sprintf("On instrument %s: %d/%d (%.2f%%)\n", latest_lcs$instrument[i], i, nrow(latest_lcs), i/nrow(latest_lcs)*100))
        
        # Get all data more recent than the latest date from the Google Drive folder
        df <- load_data(latest_lcs$folder[i],
                        device=latest_lcs$instrument[i],
                        start=latest_lcs$latest_date[i] + 1,  # Get data from the following day
                        subset=NULL)

        if (is.null(df) || nrow(df) == 0) next
        
        df_long <- melt(df, id.vars=c("timestamp", "manufacturer", "device"),
                        variable.name="measurand", value.name="measurement")
        setnames(df_long, old=c("timestamp", "device"), new=c("time", "instrument"))
        df_long[, manufacturer := NULL]
        
        if (latest_lcs$company[i] == 'Bosch') {
            measurands <- c(measurands, "NO2_1", "NO2_2", "NO2_3")
        } else if (latest_lcs$company[i] == 'Aeroqual') {
            measurands <- c(measurands, 'NO2_cal', 'O3_cal', 'PM10_cal', 'PM2.5 cal')
        } else if (latest_lcs$company[i] == 'PurpleAir') {
            measurands <- c(measurands,
                            "PM1_atm",
                            "PM1_atm_b",
                            "PM2.5_atm",
                            "PM2.5_atm_b",
                            "PM10_atm",
                            "PM10_atm_b",
                            "PM1_cf",
                            "PM1_cf_b",
                            "PM2.5_cf",
                            "PM2.5_cf_b",
                            "PM10_cf",
                            "PM10_cf_b"
                            )
        }
        
        df_long <- df_long[ measurand %in% measurands]
        
        # Add sensor number
        if (latest_lcs$company[i] %in% c("Bosch", "PurpleAir")) {
            if (latest_lcs$company[i] == "Bosch") {
                sensornumber_search <- str_match(df_long$measurand, "([[:alnum:].]+)_?([1-3]?)")
                sensor <- sensornumber_search[, 3]
                sensor <- as.numeric(ifelse(sensor == '', 1, sensor))
                df_long[, measurand := sensornumber_search[, 2] ]
                df_long[, sensornumber := sensor]
            } else if (latest_lcs$company[i] == "PurpleAir") {
                df_long[, sensornumber := ifelse(grepl("_b$", measurand), 2, 1)]
            }
        } else {
            df_long[, sensornumber := 1]
        }
        
        # Add calibrationnames, can hardcode this for PurpleAir
        if (latest_lcs$company[i] == "PurpleAir") {
            df_long[, calibrationname := 'out-of-box']
            df_long[ grepl("_atm", measurand), calibrationname := 'atmospheric' ]
            df_long[ grepl("_cf", measurand), calibrationname := 'indoor' ]
            df_long[, measurand := gsub('_.+', '', measurand) ]
        } else {
            # Get latest calibration models for each measurand
            latest_cal_product <- tbl(con, "lcsinstrument") |>
                filter(company == local(latest_lcs$company[i])) |>
                inner_join(tbl(con, "sensorcalibration"), by="instrument")
            
            # Don't automatically add new product, will need to sort this manually later
            # If don't remove it from list it will overwrite the cal2 version for the
            # 'old product' sensors
            if (latest_lcs$company[i] == 'Aeroqual') {
                latest_cal_product <- latest_cal_product |>
                    filter(calibrationname != 'new product')
            }
            
            latest_cal_product <- latest_cal_product |>
                group_by(instrument, sensornumber, measurand) |>
                filter(dateapplied == max(dateapplied, na.rm=T)) |>
                ungroup() |>
                select(instrument, sensornumber, measurand, calibrationname) |>
                collect() |>
                as.data.table()
            df_long <- latest_cal_product[df_long, on=.(instrument, measurand, sensornumber)]
            
            # Now in the new cal product calibration for Aeroqual
            if (latest_lcs$company[i] == 'Aeroqual') {
                df_long[grepl("[_ ]cal", measurand), calibrationname := 'new product']
                df_long[, measurand := gsub('[_ ]cal', '', measurand) ]
            }
            
            missing_cals <- df_long[is.na(calibrationname), ]
            if (nrow(missing_cals) > 0) {
                to_display <- missing_cals |> distinct(instrument, measurand)
                cat(sprintf("Couldn't find some calibration names for %s\n", to_display))
                next
            }
        }
        
        setcolorder(df_long, c("instrument", "measurand", "sensornumber", "calibrationname", "time", "measurement"))
        df_long <- df_long[ !is.na(measurement)]
        
        dbAppendTable(con, "measurement", df_long)
    }
}

update_gaps <- function(con) {
    # Find all days with at least one measurement per instrument/measurand combo
    df_avail <- tbl(con, "lcsinstrument") |>
                    left_join(tbl(con, "measurement"), by="instrument") |>
                    mutate(date = as_date(time)) |>
                    group_by(instrument, measurand, date) |>
                    summarise(avail=1) |>
                    collect() |>
                    ungroup()
    companies <- tbl(con, "lcscompany") |>
        inner_join(tbl(con, "lcsinstrument"), by="company") |>
        collect() |>
        ungroup() |>
        filter(company != "RLS",
               company != "PurpleAir")  # Some PA files contain timestamps that don't match the files.
                                        # Uploading these duplicate measurements causes confusion in the DB
    
    df_avail <- companies |>
        inner_join(df_avail, by="instrument") 
    
    local_folder <- tibble(study=c("QUANT", "Wider Participation"), folder=c("~/Documents/quant_data/Clean/", "~/Documents/quant_data/Clean_wider/"))
    companies <- companies |> inner_join(local_folder, by="study")  
    
    i <- 1
    for (inst in unique(df_avail$instrument)) {
        # For each instrument, find the gaps between the first and last day of data for each measurand
        cat(sprintf("Instrument: %s %d/%d (%.2f%%)\n", inst, i, length(unique(df_avail$instrument)),
                    i / length(unique(df_avail$instrument))*100))
        dir <- companies |>
            filter(instrument == inst) |>
            pull(folder)
        company <- companies |>
            filter(instrument == inst) |>
            pull(company)
        earliest_date <- df_avail |> filter(instrument == inst) |> summarise(mn=min(date)) |> pull(mn)
        latest_date <- df_avail |> filter(instrument == inst) |> summarise(mx=max(date)) |> pull(mx)
        all_dates <- as_date(seq.POSIXt(as_datetime(earliest_date), as_datetime(latest_date), by="1 day"))
        all_measurands <- df_avail |> filter(instrument == inst) |> distinct(measurand) |> pull(measurand)
        this_df <- expand.grid(date=all_dates, measurand=all_measurands) |>
            as_tibble() |>
            left_join(df_avail |> filter(instrument == inst), by=c("date", "measurand")) |>
            filter(is.na(avail))
        
        # Insert "Rescraped" calibrationname which will use when uploading data from
        # these gaps so I can manually specify which cal it is (too may factors to automate)
        rescraped_cals_in_df <- tbl(con, "sensorcalibration") |>
            filter(instrument == inst, calibrationname == "Rescraped") |>
            collect()
        if (nrow(rescraped_cals_in_df) == 0) {
            rescraped_cals <- tbl(con, "sensorcalibration") |>
                    filter(instrument == inst) |> 
                    collect() |> 
                    distinct(instrument, measurand, sensornumber) |>
                    mutate(calibrationname = "Rescraped",
                           dateapplied = as_date("2000-01-01"))
            dbAppendTable(con, "sensorcalibration", rescraped_cals)
        }
        
        # For each date of missing data, attempt to load measurements from Google Drive
        # And upload to DB using the Rescraped calibrationname
        for (this_date_num in unique(this_df$date)) {
            this_date <- as_date(this_date_num)
            cat(sprintf("On date %s\n", this_date))
            measurands <- this_df |>
                filter(date == this_date) |>
                distinct(measurand) |>
                pull(measurand)
            out <- load_data(dir, devices=inst,
                             start=this_date, end=this_date, subset=measurands)
            if (is.null(out)) next
            
            # Had some shenaningans with PurpleAir having wrong date in filename
            out <- out |>
                    filter(as_date(timestamp) == this_date)
            if (nrow(out) == 0) next

            # Convert dataframe to format for measurands table
            raw_df <- out
            df_long <- melt(raw_df, id.vars=c("timestamp", "manufacturer", "device"),
                            variable.name="measurand", value.name="measurement")
            df_long <- df_long[ !is.na(measurement)]
            setnames(df_long, old=c("timestamp", "device"), new=c("time", "instrument"))
            df_long[, manufacturer := NULL]
            
            # Add sensor number
            if (company %in% c("Bosch", "PurpleAir")) {
                if (company == "Bosch") {
                    sensornumber_search <- str_match(df_long$measurand, "([[:alnum:].]+)_?([1-3]?)")
                    sensor <- sensornumber_search[, 3]
                    sensor <- as.numeric(ifelse(sensor == '', 1, sensor))
                    df_long[, measurand := sensornumber_search[, 2] ]
                    df_long[, sensornumber := sensor]
                } else if (company == "PurpleAir") {
                    df_long[, sensornumber := ifelse(grepl("_b$", instrument), 2, 1)]
                    df_long[, instrument := gsub("_b$", "", instrument)]
                }
            } else {
                df_long[, sensornumber := 1]
            }
            
            # Add calibration name in, will manually go through and update later
            df_long[, calibrationname := "Rescraped"]
            
            # Upload! Annoyingly can't error handle
            setcolorder(df_long, c("instrument", "measurand", "sensornumber", "calibrationname", "time", "measurement"))
            dbAppendTable(con, "measurement", df_long)
        }
        i <- i + 1
    }
}

con <- dbConnect(odbc(), "QUANT")
update_chronologically(con)
update_gaps(con)

dbDisconnect(con)
