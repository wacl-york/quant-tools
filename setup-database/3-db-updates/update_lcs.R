# Insert new LCS measurements into the DB
#
# update_chronologically() looks for the latest date with data in the DB
#  for each company and attempts to insert any more recent data from the
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
               company != "PurpleAir",  # PurpleAir hasn't updated from SIM card 
               company != "RLS")        # RLS hasn't had any data
    
    latest_lcs <- tbl(con, "lcsinstrument") |>
        left_join(tbl(con, "measurement"), by="instrument") |>
        group_by(instrument) |>
        summarise(latest_date = max(time)) |>
        collect() |>
        ungroup() |>
        filter(latest_date < as_date("2022-10-01"))
    
    companies <- companies |>
        inner_join(latest_lcs, by="instrument") |>
        group_by(company, study) |>
        summarise(latest_date = max(as_date(latest_date), na.rm=T)) |>
        ungroup()
    
    local_folder <- tibble(study=c("QUANT", "Wider Participation"), folder=c("~/Documents/quant_data/Clean/", "~/Documents/quant_data/Clean_wider/"))
    companies <- companies |> inner_join(local_folder, by="study")  
    
    measurands <- tbl(con, "measurand") |>
                    collect() |>
                    pull(measurand)
    
    
    for (i in 1:nrow(companies)) {
        cat(sprintf("On company %s: %d/%d (%.2f%%)\n", companies$company[i], i, nrow(companies), i/nrow(companies)*100))
        
        # Get all data more recent than the latest date from the Google Drive folder
        df <- load_data(companies$folder[i], 
                        companies=companies$company[i], 
                        start=companies$latest_date[i] + 1,  # Get data from the following day
                        subset=NULL)
        df_long <- melt(df, id.vars=c("timestamp", "manufacturer", "device"),
                        variable.name="measurand", value.name="measurement")
        setnames(df_long, old=c("timestamp", "device"), new=c("time", "instrument"))
        df_long[, manufacturer := NULL]
        
        if (companies$company[i] == 'Bosch') {
            measurands <- c(measurands, "NO2_1", "NO2_2", "NO2_3")
        }
        df_long <- df_long[ measurand %in% measurands]
        
        # Add sensor number
        if (companies$company[i] %in% c("Bosch", "PurpleAir")) {
            if (companies$company[i] == "Bosch") {
                sensornumber_search <- str_match(df_long$measurand, "([[:alnum:].]+)_?([1-3]?)")
                sensor <- sensornumber_search[, 3]
                sensor <- as.numeric(ifelse(sensor == '', 1, sensor))
                df_long[, measurand := sensornumber_search[, 2] ]
                df_long[, sensornumber := sensor]
            } else if (companies$company[i] == "PurpleAir") {
                df_long[, sensornumber := ifelse(grepl("_b$", instrument), 2, 1)]
                df_long[, instrument := gsub("_b$", "", instrument)]
            }
        } else {
            df_long[, sensornumber := 1]
        }
        
        # Get latest calibration models for each measurand
        latest_cal_product <- tbl(con, "lcsinstrument") |>
            filter(company == local(companies$company[i])) |>
            inner_join(tbl(con, "sensorcalibration"), by="instrument") |>
            group_by(instrument, sensornumber, measurand) |>
            filter(dateapplied == max(dateapplied, na.rm=T)) |>
            ungroup() |>
            select(instrument, sensornumber, measurand, calibrationname) |>
            collect() |>
            as.data.table()
        
        # Add latest cal product name into data
        df_long <- latest_cal_product[df_long, on=.(instrument, measurand, sensornumber)]
        missing_cals <- df_long[is.na(calibrationname), ]
        if (nrow(missing_cals) > 0) {
            to_display <- missing_cals |> distinct(instrument, measurand)
            cat(sprintf("Couldn't find some calibration names for %s\n", to_display))
            next
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
        
        # Insert "Unknown" calibrationname which will use when uploading data from
        # these gaps so I can manually specify which cal it is (too may factors to automate)
        unknown_cals_in_df <- tbl(con, "sensorcalibration") |>
            filter(instrument == inst, calibrationname == "Unknown") |>
            collect()
        if (nrow(unknown_cals_in_df) == 0) {
            unknown_cals <- tbl(con, "sensorcalibration") |> 
                    filter(instrument == inst) |> 
                    collect() |> 
                    distinct(instrument, measurand, sensornumber) |>
                    mutate(calibrationname = "Unknown", 
                           dateapplied = as_date("2000-01-01"))
            dbAppendTable(con, "sensorcalibration", unknown_cals)
        }
        
        # For each date of missing data, attempt to load measurements from Google Drive
        # And upload to DB using the Unknown calibrationname
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
            df_long[, calibrationname := "Unknown"]
            
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
