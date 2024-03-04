library(tidyverse)
library(DBI)
library(RPostgres)
library(lubridate)
library(ncdf4)

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

character_categories <- list(
    calibration = list(
        "out-of-box"=1,
        "cal1"=2,
        "cal2"=3
    ),
    flag = list(
        "None"=1,
        "Info"=2,
        "Warning"=3,
        "Error"=4
    )
)

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

measurand_netcdf_metadata <- tribble(
    ~measurand, ~ncdf_name, ~standard_name, ~long_name, ~units, ~chemical_species,
    "NO2", "no2", "mass_fraction_of_nitrogen_dioxide_in_air", "Mass Fraction of Nitrogen Dioxide in air", "ppb", "NO2",
    "NO", "no", "mass_fraction_of_nitrogen_oxide_in_air", "Mass Fraction of Nitrogen Oxide in air", "ppb", "NO",
    "O3", "o3", "mass_fraction_of_ozone_in_air", "Mass Fraction of Ozone in air", "ppb", "O3",
    "CO", "co", "mass_fraction_of_carbon_monoxide_in_air", "Mass Fraction of Carbon Monoxide in air", "ppb", "CO",
    "CO2", "co2", "mass_fraction_of_carbon_dioxide_in_air", "Mass Fraction of Carbon Dioxide in air", "ppm", "CO2",
    "PM1", "pm1", "mass_concentration_of_pm1_ambient_aerosol_in_air", "Mass Concentration of PM1 Ambient Aerosol in air", "ug m-3", "",
    "PM2.5", "pm2p5", "mass_concentration_of_pm2p5_ambient_aerosol_in_air", "Mass Concentration of PM2.5 Ambient Aerosol in air", "ug m-3", "",
    "PM10", "pm10", "mass_concentration_of_pm10_ambient_aerosol_in_air", "Mass Concentration of PM10 Ambient Aerosol in air", "ug m-3", "",
    "Temperature", "air_temperature", "air_temperature", "Air Temperature", "K", "",
    "RelHumidity", "relative_humidity", "relative_humidity", "Relative Humidity", "%", "",
    "Pressure", "air_pressure", "air_pressure", "Air Pressure", "hPa", ""
    )
    
export_to_netcdf <- function(in_instrument, in_measurand, in_location, out_dir, version=1) {
    # Get data from DB 
    if (in_measurand == 'PM') {
        in_measurand <- c('PM1', 'PM2.5', 'PM4', 'PM10')
        measurand_title <- 'PM'
        ncdf_title <- 'Particulate Matter Concentration'
    } else if (in_measurand == 'Met') {
        in_measurand <- c('Temperature', 'RelHumidity', 'Pressure')
        measurand_title <- 'Met'
        ncdf_title <- 'Surface Meteorology'
    } else {
        measurand_title <- in_measurand
        ncdf_title <- sprintf("%s Concentration", in_measurand)
    }
    
    df <- tbl(con, "lcs") |> 
            filter(instrument == in_instrument,
                   measurand %in% in_measurand,
                   location == in_location) |> 
            collect()
    
    # Get metadata
    company_meta <- tbl(con, "lcsinstrument") |>
        filter(instrument == in_instrument) |>
        inner_join(tbl(con, "lcscompany"), by="company") |>
        collect()
    
    if (company_meta$company == 'PurpleAir' && measurand_title == 'PM') {
        character_categories$calibration <- list(
            atmospheric=1,
            indoor=2
        )
    }
    
    first_date <- as_date(min(df$time))
    end_date <- as_date(max(df$time))
    
    filename <- sprintf("%s/%s/%s-%s-%s_%s_%s-%s.nc",
                        out_dir,
                        in_location,
                        gsub(" ", "", company_meta$companyhumanreadable),
                        in_instrument,
                        measurand_title,
                        in_location,
                        first_date,
                        end_date)

    # Clean up strings to make them consistent and remove NAs
    df <- df |>
        rename(
            flag_char = flag,
            cal_char = version,
            flagreason_char = flagreason
       )
    
    df_wide <- df |>
        pivot_wider(names_from=measurand, values_from=c(measurement, flag_char, flagreason_char)) |>
        mutate(
            across(starts_with("flag_char"), function(x) ifelse(is.na(x), 'None', x)),
            across(starts_with("flag_char"), function(x) gsub(",.+", "", x)),
            across(starts_with("flagreason_char"), function(x) ifelse(is.na(x), 'None', x))
        )
    
    # Scale temperature
    if ('measurement_Temperature' %in% colnames(df_wide)) {
        df_wide[['measurement_Temperature']] <- celsius_to_kelvin(df_wide[['measurement_Temperature']])
    }
    
    # Convert characters to enums
    # Don't have hardcoded order for flag reasons, but just ensure that None is at the start
    flagreason_categories <- unique(df$flagreason_char)
    flagreason_categories <- c('None', setdiff(flagreason_categories, c('None', NA)))
    
    df_wide <- df_wide |>
        mutate(
            cal_num = factor(cal_char, 
                             levels=names(character_categories$calibration),
                             labels=character_categories$calibration
            ),
            across(starts_with("flag_char"), 
                   function(x) factor(x, levels=names(character_categories$flag), labels=character_categories$flag),
                   .names="{gsub('char', 'num', .col)}"),
            across(starts_with("flagreason_char"),
                   function(x) factor(x, levels=flagreason_categories, labels=seq_along(flagreason_categories)),
                   .names="{gsub('char', 'num', .col)}")
        )
    
    # Add fill value and flag for missing values
    # Doing this here as could have missing values added from pivoting
    for (x in unique(df$measurand)) {
        df_wide[[sprintf("flag_num_%s", x)]][is.na(df_wide[[sprintf("measurement_%s", x)]])] <- 4
        df_wide[[sprintf("measurement_%s", x)]][is.na(df_wide[[sprintf("measurement_%s", x)]])] <- FILL_VALUE
    }
    
    # Create NetCDF object
    dim_time <- ncdim_def("time",
                          units="seconds since 1970-01-01 00:00:00",
                          longname="time",
                          vals=as.integer(df_wide$time),
                          unlim=TRUE)
    var_sensornumber <- ncvar_def("sensornumber",
                                  units="",
                                  dim=dim_time,
                                  longname="Sensor Number",
                                  prec="short"
    )
    var_cal <- ncvar_def("calibration",
                         units="",
                         dim=dim_time,
                         longname="Calibration algorithm",
                         prec="short"
    )
    # create one per each measurement unit
    measurands <- setNames(unique(df$measurand), unique(df$measurand))
        
    measurement_vars <- lapply(measurands, function(x) {
        meta <- measurand_netcdf_metadata |> filter(measurand == x)
        ncvar_def(meta$ncdf_name,
                  units=meta$units,
                  dim=dim_time,
                  missval=FILL_VALUE,
                  longname=meta$long_name,
                  prec="double"
        )
    })
    
    flag_vars <- lapply(measurands, function(x) {
        meta <- measurand_netcdf_metadata |> filter(measurand == x)
        ncvar_def(sprintf("qc_flag_%s", meta$ncdf_name),
                          units="",
                          dim=dim_time,
                          longname=sprintf("%s QC Flag", x),
                          prec="short"
        )
    })
    
    flagreason_vars <- lapply(measurands, function(x) {
        meta <- measurand_netcdf_metadata |> filter(measurand == x)
        ncvar_def(sprintf("qc_flag_reason_%s", meta$ncdf_name),
                          units="",
                          dim=dim_time,
                          longname=sprintf("%s QC Flag Explanation", x),
                          prec="short"
        )
    })
    
    # Create obj
    obj <- nc_create(filename, 
                     c(
                         list(var_sensornumber, var_cal),
                         measurement_vars,
                         flag_vars,
                         flagreason_vars
                     )
    )
    
    # Metadata for time
    ncatt_put(obj, "time", attname="type", attval="int")
    ncatt_put(obj, "time", attname="valid_min", attval=min(df_wide$time, na.rm=T))
    ncatt_put(obj, "time", attname="valid_max", attval=max(df_wide$time, na.rm=T))
    
    # Add metadata for categorical variables
    ncatt_put(obj, var_cal, attname="type", attval="short")
    ncatt_put(obj, var_sensornumber, attname="type", attval="short")
    ncatt_put(obj, var_cal, attname="flag_values", attval=paste(character_categories$calibration, collapse=' '))
    ncatt_put(obj, var_cal, attname="flag_meanings", attval=paste(names(character_categories$calibration), collapse=' '))
    ncatt_put(obj, var_cal, attname="valid_min", attval=min(unlist(character_categories$calibration)))
    ncatt_put(obj, var_cal, attname="valid_max", attval=max(unlist(character_categories$calibration)))
    ncatt_put(obj, var_sensornumber, attname="valid_min", attval=min(df_wide$sensornumber))
    ncatt_put(obj, var_sensornumber, attname="valid_max", attval=max(df_wide$sensornumber))
    cals_applied <- tbl(con, "sensorcalibration") |>
                        filter(instrument == in_instrument, measurand %in% in_measurand) |>
                        distinct(calibrationname, dateapplied) |>
                        collect() |>
                        mutate(calibrationname = factor(calibrationname, levels=names(character_categories$calibration))) |>
                        arrange(calibrationname) |>
                        mutate(dateapplied = format_ISO8601(dateapplied, usetz="Z")) |>
                        pull(dateapplied)
    first_sensor_measurements <- df_wide |> 
                                    group_by(sensornumber) |>
                                    summarise(
                                        earliest = format_ISO8601(min(time, na.rm=T), usetz="Z"),
                                        latest = format_ISO8601(max(time, na.rm=T), usetz="Z")
                                    ) |>
                                    ungroup() |>
                                    arrange(sensornumber) 
    ncatt_put(obj, var_cal, attname="date_calibration_applied", attval=paste(cals_applied, collapse=' '))
    ncatt_put(obj, var_sensornumber, attname="date_first_measurement", attval=paste(first_sensor_measurements$earliest, collapse=' '))
    ncatt_put(obj, var_sensornumber, attname="date_last_measurement", attval=paste(first_sensor_measurements$latest, collapse=' '))
    
    # add metadata for measurements and flags per measurement
    result <- lapply(measurands, function(x) {
        meta <- measurand_netcdf_metadata |> filter(measurand == x)
        raw_vals <- df_wide[[sprintf("measurement_%s", x)]]
        non_flagged <- raw_vals[df_wide[[sprintf("flag_num_%s", x)]] == 1]
        ncatt_put(obj, 
                  measurement_vars[[x]], 
                  attname="type", 
                  attval="double"
        )
        ncatt_put(obj, 
                  measurement_vars[[x]], 
                  attname="standard_name", 
                  attval=meta$standard_name
        )
        ncatt_put(obj, 
                  measurement_vars[[x]], 
                  attname="valid_min", 
                  attval=min(non_flagged, na.rm=T)
        )
        ncatt_put(obj, 
                  measurement_vars[[x]], 
                  attname="valid_max", 
                  attval=max(non_flagged, na.rm=T)
        )
        ncatt_put(obj, 
                  measurement_vars[[x]], 
                  attname="coordinates", 
                  attval=sprintf("%fN %fE", locations[[in_location]]$lat, locations[[in_location]]$lon)
        )
        if (meta$chemical_species != '') {
            ncatt_put(obj, 
                      measurement_vars[[x]], 
                      attname="chemical_species", 
                      attval=meta$chemical_species
            )
        }
        
        # Flag meta
        ncatt_put(obj, flag_vars[[x]], attname="type", attval="short")
        ncatt_put(obj, flag_vars[[x]], attname="valid_min", attval=min(unlist(character_categories$flag)))
        ncatt_put(obj, flag_vars[[x]], attname="valid_max", attval=max(unlist(character_categories$flag)))
        ncatt_put(obj, flag_vars[[x]], attname="flag_values", attval=paste(character_categories$flag, collapse=' '))
        ncatt_put(obj, flag_vars[[x]], attname="flag_meanings", attval=paste(names(character_categories$flag), collapse=' '))
        ncatt_put(obj, flagreason_vars[[x]], attname="type", attval="short")
        ncatt_put(obj, flagreason_vars[[x]], attname="valid_min", attval=1)
        ncatt_put(obj, flagreason_vars[[x]], attname="valid_max", attval=length(flagreason_categories))
        ncatt_put(obj, flagreason_vars[[x]], attname="flag_values", attval=paste(seq_along(flagreason_categories), collapse=' '))
        ncatt_put(obj, flagreason_vars[[x]], attname="flag_meanings", attval=paste(gsub(" ", "_", flagreason_categories), collapse=' '))
        
        # ancillary variables to explicitly link measurements and flags
        ncatt_put(obj, 
                  measurement_vars[[x]], 
                  attname="ancilliary_variables", 
                  attval=sprintf("qc_flag_%s qc_flag_reason_%s", meta$ncdf_name, meta$ncdf_name)
        )
    })
    
    # Global metadata
    global_meta <- list(
        Conventions="CF-1.6",
        title=ncdf_title,
        institution="Wolfson Atmospheric Chemistry Laboratories, University of York",
        source=sprintf("Surface observations of %s from a %s low-cost instrument", 
                       ncdf_title, company_meta$companyhumanreadable),
        history=sprintf("Release %d: %s", version, lubridate::today()),
        references="Diez, S., Lacy, S. E., Bannan, T. J., Flynn, M., Gardiner, T., Harrison, D., ... & Edwards, P. M. (2022). Air pollution measurement errors: is your data fit for purpose?. Atmospheric Measurement Techniques, 15(13), 4091-4105.",
        comment="",
        featureType="timeSeries",
        instrument_url=company_meta$url,
        instrument_manufacturer=company_meta$companyhumanreadable,
        instrument_model=company_meta$modelname,
        instrument_studyid=in_instrument,
        averaging_interval=company_meta$averagingperiod,
        project="Quantification of Utility of Atmospheric Network Technologies (QUANT)",
        project_url="www.shiny.york.ac.uk/quant",
        project_principal_investigator="Pete Edwards",
        project_principal_investigator_email="pete.edwards@york.ac.uk",
        project_principal_investigator_url="https://www.york.ac.uk/chemistry/staff/academic/d-g/edwardsp/",
        project_study=company_meta$study,
        creator_name="Stuart Lacy",
        creator_email="stuart.lacy@york.ac.uk",
        creator_url="https://pure.york.ac.uk/portal/en/persons/stuart-edward-lacy",
        time_coverage_start=format_ISO8601(min(df_wide$time, na.rm=T), usetz = "Z"),
        time_coverage_end=format_ISO8601(max(df_wide$time, na.rm=T), usetz="Z"),
        last_revised_date=format_ISO8601(lubridate::now(), usetz="Z"),
        licence="Data usage licence - UK Government Open Licence agreement: http://www.nationalarchives.gov.uk/doc/open-government-licence",
        acknowledgement="Acknowledgement of NERC and QUANT as the data providers is required whenever and wherever this data is used"
        # The MAQS files have these following fields, but they aren't in the CF standard
        #instrument_serial_number="",
        #instrument_software="",
        #instrument_software_version="",
        #processing_software_url="",
        #processing_software_version="",
        #calibration_sensitivity="",
        #calibration_certification_url="",
        #sampling_interval="",
        #product_version="",
        #processing_level="",
        #platform="",
        #platform_type="",
        #deployment_mode="",
        #geospatial_bounds="",
        #platform_altitude="",
        #location_keywords="",
        #amf_vocabularies_release="",
    )
    for (x in names(global_meta)) {
        # 0 varid signifies global
        ncatt_put(obj, varid=0, attname=x, attval=global_meta[[x]])
    }
    
    # Add data!
    ncvar_put(obj, var_sensornumber, df_wide$sensornumber)
    ncvar_put(obj, var_cal, df_wide$cal_num)
    
    results <- lapply(measurands, function(x) {
        ncvar_put(obj, measurement_vars[[x]], df_wide[[sprintf("measurement_%s", x)]])
        ncvar_put(obj, flag_vars[[x]], df_wide[[sprintf("flag_num_%s", x)]])
        ncvar_put(obj, flagreason_vars[[x]], df_wide[[sprintf("flagreason_num_%s", x)]])
    })
    
    nc_close(obj)
}

export_to_netcdf(this_job$instrument, this_job$measurand, this_job$location, this_job$out_dir, this_job$version)

dbDisconnect(con)
