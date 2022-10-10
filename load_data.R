library(data.table)
library(parallel)
library(lubridate)

#' Extracts the recording data from a QUANT filename.
#'
#' QUANT clean data files are named according to the convention
#' "<manufacturer>_<device>_<date>.csv", where <date> is in YYYY-mm-dd format.
#'
#' Args:
#'     - filename (str): The CSV filename.
#'
#' Returns:
#'     The date of recording as a Date object.
get_date_from_filename <- function(filename) {
    # Remove directories to just get base filename
    base <- basename(filename)
    # Remove file extension
    fn <- tools::file_path_sans_ext(base)

    # Extract date, assuming syntax <manufacturer>_<device>_<date>
    fn_split <- strsplit(fn, "_")[[1]]
    if (length(fn_split) != 3) {
        return(NA)
    }
    date_str <- fn_split[3]

    # Parse date from string. Returns NA if parse failed
    dt <- lubridate::as_date(date_str, format="%Y-%m-%d")
    dt
}

#' Loads a single CSV file as a data.table.
#'
#' After reading the file into memory, duplicate rows are dropped and any
#' required resampling is performed.
#'
#' Args:
#'     - filename (str): Filename to load. Must be a CSV.
#'     - resample (str): Resampling format to use as specified by
#'         lubridate::floor_date. If NULL then doesn't do any resampling.
#'
#' Returns:
#'     A data.table in long format with 3 columns:
#'         - timestamp
#'         - measurand
#'         - value
load_file <- function(fn, resample=NULL) {
    df <- fread(fn)
    df <- unique(df, by=c('timestamp', 'measurand'))
    df[, timestamp := as_datetime(timestamp)]

    if (!is.null(resample)) {
        df[, timestamp := floor_date(timestamp, unit=resample)]
        df <- df[, .(value = mean(value)), by=c('timestamp', 'measurand') ]
    }
    df[, c("manufacturer", "device") := tstrsplit(basename(fn), "_", fixed=TRUE)[1:2]]
    df
}

#' Loads QUANT data from multiple files into a single data.table.
#'
#' Args:
#'     - folder (str): The folder containing all the CSV files. This should be
#'         a mirror of the QUANT/Data/Clean GoogleDrive folder.
#'     - companies (character vector): A list of companies to load data from. Can contain
#'     values: 'Aeroqual', 'AQMesh', 'Zephyr', 'QuantAQ'.
#'     - devices (character vector): A list of devices to load data from, i.e: `AQY872`, `AQM391`, `Zep188`, `Ari063`. Both `companies` and `devices` can be provided.
#'     - start (str): The earliest date to include data from, in YYYY-mm-dd
#'         format. If not provided then uses the earliest available date.
#'     - end (str): The latest date to include data from, in YYYY-mm-dd
#'         format. If not provided then uses the latest available date.
#'     - resample (str): Resampling format to use as specified by
#'         lubridate::floor_date. If NULL then doesn't do any resampling.
#'     - subset (character vector): A list of pollutants to include in the final data
#'         frame. If NULL then returns all.
#'     - n_cores (int): Number of cores to use when loading data. NB: DOESN'T WORK ON WINDOWS.
#'         If n_cores < 1 (as in the default), then the program is run in a single thread
#'
#' Returns:
#'     A data.table with 1 row per observation per device, resampled to the
#'     specified frequency. There are manufacturer and device columns to index
#'     where the recording was made, and there are measurement columns for each
#'     pollutant of interest.
load_data <- function(folder, 
                      companies=NULL,
                      devices=NULL,
                      start=NULL,
                      end=NULL,
                      resample="1 minute",
                      subset=c('NO', 'NO2', 'O3', 'CO2', 'CO', 'Temperature', 'RelHumidity'),
                      n_cores=-1
) {
    if (substr(folder, nchar(folder), nchar(folder)) != '/') {
        folder <- sprintf("%s/", folder)
    }
    
    # Find specified files
    if (is.null(companies) && is.null(devices)) {
        fns <- Sys.glob(sprintf("%s*.csv", folder))
    } else {
        fns <- c()
        if (!is.null(companies)) {
            company_fns <- unname(unlist(sapply(companies, function(x) Sys.glob(sprintf("%s/%s_*.csv", folder, x)))))
            fns <- c(fns, company_fns)
        }
        if (!is.null(devices)) {
            device_fns <- unname(unlist(sapply(devices, function(x) Sys.glob(sprintf("%s/*_%s_*.csv", folder, x)))))
            fns <- c(fns, device_fns)
        }
    }
    
    # Subset to dates of interest
    if (!is.null(start)) {
        start_dt <- lubridate::as_date(start, format="%Y-%m-%d")
        fns <- fns[sapply(fns, get_date_from_filename) >= start_dt]
    }
    if (!is.null(end)) {
        end_dt <- lubridate::as_date(end, format="%Y-%m-%d")
        fns <- fns[sapply(fns, get_date_from_filename) <= end_dt]
    }
    
    if (length(fns) == 0) {
        message("No filenames found that match criteria.")
        return(NULL)
    }
    
    # Read into 1 data table at once
    if (n_cores > 0) {
        df_2 <- rbindlist(mclapply(fns, load_file, resample=resample, mc.cores=n_cores))
    } else {
        df_2 <- rbindlist(lapply(fns, load_file, resample=resample))
    }

    # Drop duplicate values
    df_2 <- unique(df_2, by=c('timestamp', 'measurand', 'manufacturer', 'device'))
    
    # Renaming temperature measurements
    df_2[ measurand == 'Temperature' & manufacturer == 'Zephyr', measurand := 'TempPCB' ]
    df_2[ measurand == 'TempAmb' & manufacturer == 'Zephyr', measurand := 'Temperature' ]
    df_2[ measurand == 'RelHumidity' & manufacturer == 'Zephyr', measurand := 'RelHumPCB' ]
    df_2[ measurand == 'RelHumAmb' & manufacturer == 'Zephyr', measurand := 'RelHumidity' ]
    df_2[ measurand == 'TempMan' & manufacturer == 'QuantAQ', measurand := 'Temperature' ]
    df_2[ measurand == 'RelHumMan' & manufacturer == 'QuantAQ', measurand := 'RelHumidity' ]
    df_2[ measurand == 'current_temp_f' & manufacturer == 'PurpleAir', measurand := 'Temperature_F' ]
    df_2[ measurand == 'current_humidity' & manufacturer == 'PurpleAir', measurand := 'RelHumidity' ]
    df_2[ measurand == 'pm1_0_atm' & manufacturer == 'PurpleAir', measurand := 'PM1' ]
    df_2[ measurand == 'pm2_5_atm' & manufacturer == 'PurpleAir', measurand := 'PM2.5' ]
    df_2[ measurand == 'pm10_0_atm' & manufacturer == 'PurpleAir', measurand := 'PM10' ]
    df_2[ measurand == 'pm1_0_atm_b' & manufacturer == 'PurpleAir', measurand := 'PM1_b' ]
    df_2[ measurand == 'pm2_5_atm_b' & manufacturer == 'PurpleAir', measurand := 'PM2.5_b' ]
    df_2[ measurand == 'pm10_0_atm_b' & manufacturer == 'PurpleAir', measurand := 'PM10_b' ]

    if (!is.null(subset))
        df_2 <- df_2[ measurand %in% subset ]
        
    if (nrow(df_2) > 0) {
        out <- dcast(df_2, timestamp + manufacturer + device  ~ measurand)
    } else {
        out <- NULL
    }
    out
}
