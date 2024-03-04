library(tidyverse)
library(DBI)
library(RPostgres)
library(lubridate)

FILL_VALUE <- -9999

# Get the parameters for this specific run
args <- commandArgs(trailingOnly = TRUE)
row_num <- args[1]
lcs_jobs <- read_csv("lcs_job_list.csv")
this_job <- lcs_jobs[row_num, ]

con <- dbConnect(RPostgres::Postgres(), 
                 host="",
                 dbname="",
                 user="",
                 password=""
)

celsius_to_kelvin <- function(x) x + 273.15

cal_order <- c("out-of-box", "cal1", "cal2", "atmospheric", "indoor")

locations <- list(
    Manchester = list(
        "lat"=53.444222,
        "lon"=-2.214417
    ),
    London = list(
        "lat"=51.449694,
        "lon"=-0.037389
    ),
    York = list(
        "lat"=53.951917,
        "lon"=-1.075861
    )
)

export_to_csv <- function(in_instrument, in_measurand, in_location, out_dir, version=1) {
    # Get data from DB 
    if (in_measurand == 'PM') {
        in_measurand <- c('PM1', 'PM2.5', 'PM4', 'PM10')
        measurand_title <- 'PM'
    } else if (in_measurand == 'Met') {
        in_measurand <- c('Temperature', 'RelHumidity', 'Pressure')
        measurand_title <- 'Met'
    } else {
        measurand_title <- in_measurand
    }
    
    df <- tbl(con, "lcs") |> 
            filter(instrument == in_instrument,
                   measurand %in% in_measurand,
                   location == in_location) |> 
            rename(calibration = version) |>
            collect() |>
            mutate(calibration = factor(calibration, levels=cal_order))
    
    # Get metadata
    company_meta <- tbl(con, "lcsinstrument") |>
        filter(instrument == in_instrument) |>
        inner_join(tbl(con, "lcscompany"), by="company") |>
        collect()
    
    first_date <- as_date(min(df$time))
    end_date <- as_date(max(df$time))
    
    filename <- sprintf("%s/%s/%s_%s_%s_%s_%s_%s.csv",
                        out_dir,
                        in_location,
                        gsub(" ", "", company_meta$companyhumanreadable),
                        in_instrument,
                        measurand_title,
                        in_location,
                        first_date,
                        end_date)

    # Clean up strings to make them consistent and remove NAs
    df_wide <- df |>
        pivot_wider(names_from=measurand, values_from=c(measurement, flag, flagreason)) |>
        mutate(
            across(starts_with("flag"), function(x) ifelse(is.na(x), 'None', x)),
            across(starts_with("flag"), function(x) gsub(",.+", "", x)),
            across(starts_with("flagreason"), function(x) ifelse(is.na(x), 'None', x))
        ) |>
        arrange(calibration, time)
    
    # Scale temperature
    if ('measurement_Temperature' %in% colnames(df_wide)) {
        df_wide[['measurement_Temperature']] <- celsius_to_kelvin(df_wide[['measurement_Temperature']])
    }
    
    # Add fill value and flag for missing values
    # Doing this here as could have missing values added from pivoting
    for (x in unique(df$measurand)) {
        df_wide[[sprintf("flag_%s", x)]][is.na(df_wide[[sprintf("measurement_%s", x)]])] <- "Error"
        df_wide[[sprintf("measurement_%s", x)]][is.na(df_wide[[sprintf("measurement_%s", x)]])] <- FILL_VALUE
    }
    
    write_csv(df_wide, filename)
}

export_to_csv(this_job$instrument, this_job$measurand, this_job$location, this_job$out_dir, this_job$version)

dbDisconnect(con)
