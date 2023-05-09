# https://cfconventions.org/cf-conventions/cf-conventions.html
# Climate and Forecasting Conventions as recommended by CEDA
library(tidyverse)
library(odbc)
library(lubridate)
library(ncdf4)

con <- dbConnect(odbc(), "QUANT")
df <- tbl(con, "lcs_hourly") |> filter(instrument == 'AQM388') |> collect()
cal_ord <- tbl(con, "sensorcalibration") |>
    filter(instrument == 'AQM388') |>
    select(-instrument) |>
    collect()
no2_cals <- cal_ord |> filter(measurand == 'NO2') |> arrange(dateapplied)

# Clean up strings to make them consistent and remove NAs
df <- df |>
    rename(flag_char = flag,
           cal_char = version,
           flagreason_char = flagreason) |>
    mutate(
        flag_char = ifelse(is.na(flag_char), 'None', flag_char),
        flag_char = gsub(",.+", "", flag_char),
        flagreason_char = ifelse(is.na(flagreason_char), 'NA', flagreason_char),
           )

# Convert characters to enums
cal_categories <- c('out-of-box', 'cal1', 'cal2')
flag_categories <- c('None', 'Info', 'Warning', 'Error')
flagreason_categories <- unique(df$flagreason_char)
# Ensure NA is at the start
flagreason_categories <- c('NA', setdiff(flagreason_categories, 'NA'))

df <- df |>
    mutate(
        cal_num = factor(cal_char, 
                         levels=cal_categories,
                         labels=seq_along(cal_categories)),
        flag_num = factor(flag_char,
                          levels=flag_categories,
                          labels=seq_along(flag_categories)),
        flagreason_num = factor(flagreason_char,
                                levels=flagreason_categories,
                                labels=seq_along(flagreason_categories))
    )

# just try with 1 measurand 
df_no2 <- df |> filter(measurand == 'NO2') |> select(-measurand) |> rename(NO2 = measurement)

######################## NEW VERSION
# Setup proprely as an 'indexed ragged array representation', as shown in H.2.5
dim_obs <- ncdim_def("obs", units="", vals=1:nrow(df_no2), unlim=TRUE, create_dimvar = FALSE)
dim_sensor <- ncdim_def("sensor", units="", vals=unique(df_no2$sensornumber), create_dimvar=FALSE)
dim_cal <- ncdim_def("calibration_algorithm", units="", vals=seq_along(cal_categories), create_dimvar = FALSE)
dim_calname <- ncdim_def("calibration_nchar", units="", vals=seq(max(str_length(cal_categories))))

var_calname <- ncvar_def("calibration_name", "", list(dim_calname, dim_cal), " ", prec="char")
var_caldate <- ncvar_def("calibration_date_applied", units="seconds since 1970-01-01 00:00:00", dim=dim_cal, prec="integer")
var_sensordate <- ncvar_def("sensor_installation_date", units="seconds since 1970-01-01 00:00:00", dim=dim_sensor, prec="integer")
var_sensorIndex <- ncvar_def("sensorIndex", units="", dim=dim_obs, prec="short")
var_calIndex <- ncvar_def("calIndex", units="", dim=dim_obs, prec="short")
var_time <- ncvar_def("time", units="seconds since 1970-01-01 00:00:00", dim=dim_obs, prec="integer")
var_measurement <- ncvar_def("no2_measurement", units="ppb", dim=dim_obs)
var_flagreason <- ncvar_def("flag_reason", units="", dim=dim_obs, prec="short")
var_flagtype <- ncvar_def("flag_type", units="", dim=dim_obs, prec="short")

# When add attrs, add instance_dimension to sensorIndex and calIndex
obj_jagged <- nc_create("aqm388_no2_jagged.nc", list(var_calname, var_caldate, var_sensordate, var_calIndex, var_sensorIndex, var_time, var_measurement, var_flagtype, var_flagreason))
ncatt_put(obj_jagged, var_calIndex, attname="instance_dimension", attval="calibration_algorithm")
ncatt_put(obj_jagged, var_sensorIndex, attname="instance_dimension", attval="sensor")

# Add metadata about categorical variables!
ncatt_put(obj_jagged, var_flagtype, attname="flag_values", attval=paste(seq_along(flag_categories), collapse=' '))
ncatt_put(obj_jagged, var_flagtype, attname="flag_meanings", attval=paste(flag_categories, collapse=' '))
ncatt_put(obj_jagged, var_flagreason, attname="flag_values", attval=paste(seq_along(flagreason_categories), collapse=' '))
ncatt_put(obj_jagged, var_flagreason, attname="flag_meanings", attval=paste(flagreason_categories, collapse=' '))

# Now add data
ncvar_put(obj_jagged, var_calname, no2_cals$calibrationname)
ncvar_put(obj_jagged, var_caldate, no2_cals$dateapplied)
ncvar_put(obj_jagged, var_sensordate, min(no2_cals$dateapplied))
ncvar_put(obj_jagged, var_sensorIndex, df_no2$sensornumber, count=nrow(df_no2))
ncvar_put(obj_jagged, var_calIndex, df_no2$cal_num, count=nrow(df_no2))
ncvar_put(obj_jagged, var_time, df_no2$time, count=nrow(df_no2))
ncvar_put(obj_jagged, var_measurement, df_no2$NO2, count=nrow(df_no2))
ncvar_put(obj_jagged, var_flagtype, df_no2$flag_num, count=nrow(df_no2))
ncvar_put(obj_jagged, var_flagreason, df_no2$flagreason_num, count=nrow(df_no2))

nc_close(obj_jagged)

# Try opening with tidync
library(tidync)
src_jag <- tidync("aqm388_no2_jagged.nc", "D3")
nc_df_jag <- src_jag |> hyper_tibble() |>
            mutate(time=as_datetime(time))
# Can't seem to automatically get cal name due to how the character name is stored!
nc_df_jag

# Calibration version - awkward to read
tidync("aqm388_no2_jagged.nc", "D0,D1") |>
    hyper_tibble()

# Date calibration was applied
tidync("aqm388_no2_jagged.nc", "D1") |>
    hyper_tibble()

# Date sensor was installed
tidync("aqm388_no2_jagged.nc", "D2") |>
    hyper_tibble()

# Attributes are as normal
ncmeta::nc_atts("aqm388_no2_jagged.nc") |> unnest(cols=c(value))

####################################################################


######################## OLD LONG VERSION
# Now create variables
dim_time <- ncdim_def("time",
                      units="seconds since 1970-01-01",
                      longname="time",
                      vals=as.integer(df_no2$time),
                      unlim=TRUE)
var_sensornumber <- ncvar_def("sensornumber",
                              units="count",
                              dim=dim_time,
                              longname="Sensor Number",
                              prec="short"
)
var_measurement <- ncvar_def("measurement",
                             units="ppb",
                             dim=dim_time,
                             longname="measurement",
                             prec="double"
)

var_cal <- ncvar_def("calibration",
                     units="",
                     dim=dim_time,
                     longname="Calibration algorithm",
                     prec="short"
)

var_flagtype <- ncvar_def("flag_type",
                     units="",
                     dim=dim_time,
                     longname="Flag severity",
                     prec="short"
)
var_flagreason <- ncvar_def("flag",
                     units="",
                     dim=dim_time,
                     longname="Status flag",
                     prec="short"
)
# Create obj
obj <- nc_create("aqm388_no2.nc", list(var_sensornumber, var_measurement, var_cal, var_flagtype, var_flagreason))

# Add metadata about categorical variables!
ncatt_put(obj, var_cal, attname="flag_values", attval=paste(seq_along(cal_categories), collapse=' '))
ncatt_put(obj, var_cal, attname="flag_meanings", attval=paste(cal_categories, collapse=' '))
ncatt_put(obj, var_cal, attname="date_applied", attval=paste(no2_cals$dateapplied, collapse=' '))
ncatt_put(obj, var_sensornumber, attname="date_applied", attval=paste(min(no2_cals$dateapplied)))
ncatt_put(obj, var_flagtype, attname="flag_values", attval=paste(seq_along(flag_categories), collapse=' '))
ncatt_put(obj, var_flagtype, attname="flag_meanings", attval=paste(flag_categories, collapse=' '))
ncatt_put(obj, var_flagreason, attname="flag_values", attval=paste(seq_along(flagreason_categories), collapse=' '))
ncatt_put(obj, var_flagreason, attname="flag_meanings", attval=paste(flagreason_categories, collapse=' '))

# Add data!
ncvar_put(obj, var_measurement, df_no2$NO2)
ncvar_put(obj, var_sensornumber, df_no2$sensornumber)
ncvar_put(obj, var_cal, df_no2$cal_num)
ncvar_put(obj, var_flagtype, df_no2$flag_num)
ncvar_put(obj, var_flagreason, df_no2$flagreason_num)

nc_close(obj)

# Try opening with tidync
src <- tidync("aqm388_no2.nc")
nc_df <- src |> hyper_tibble() |>
            mutate(time=as_datetime(time))
nc_df
# Can see here have duplicate times which is good! Was worried would lose this

# Can I see attributes to get the levels? 
ncmeta::nc_atts("aqm388_no2.nc") |>
    unnest(cols=c(value))
########################################################################################

##################################### ALL MEASUREMENTS FULLY WIDE 
# Trying full wide specification with all measurands
df_wide <- df |>
            select(-location, -instrument, -flag_char, -flagreason_char, -cal_num) |>
            rename(cal=cal_char, flag=flag_num, flagreason=flagreason_num) |>
            pivot_wider(id_cols = c(time),
                        names_from=c(measurand, cal, sensornumber), 
                        values_from=c(measurement, flag, flagreason))

# Get variables in order we want to display to users!
all_cols <- colnames(df_wide |> select(-time))
df_ord <- str_match(all_cols, "(.+)_(.+)_(.+)_(.+)") |>
    as.data.frame() |>
    as_tibble()
colnames(df_ord) <- c('raw', 'type', 'measurand', 'calibrationname', 'sensornumber')
df_ord$sensornumber <- as.integer(df_ord$sensornumber)
measurand_ord <- c('NO2', 'O3', 'NO', 'CO2', 'PM2.5', 'PM1', 'PM10', 'Temperature', 'RelHumidity')
type_ord <- c('measurement', 'flag', 'flagreason')
all_cols_ord <- df_ord |>
    inner_join(cal_ord, by=c('measurand', 'calibrationname', 'sensornumber')) |>
    mutate(measurand = factor(measurand, levels=measurand_ord),
           type = factor(type, levels=type_ord)) |>
    arrange(type, measurand, dateapplied) |>
    pull(raw)

dim_time_wide <- ncdim_def("time",
                          units="seconds since 1970-01-01",
                          longname="time",
                          vals=as.integer(df_wide$time),
                          unlim=TRUE)

types <- list(
    "measurement" = 
        list(
            precision="double"
        ),
    "flag" = 
        list(
            precision="short"
        ),
    "flagreason" = 
        list(
            precision="short"
        )
)

create_var <- function(x) {
    regex <- str_match(x, "(.+)_(.+)_(.+)_(.+)")
    type <- regex[2]
    measurand <- regex[3]
    cal <- regex[4]
    sensor <- regex[5]
    
    # CF standards uses kgm-3
    # http://cfconventions.org/Data/cf-standard-names/current/src/cf-standard-name-table.xml
    if (type == 'measurement') {
        units <- tbl(con, "measurand") |>
                    filter(measurand == local(measurand)) |>
                    pull(units)
        miss <- NA
    } else {
        units <- ''
        miss <- -1
    }
    
    ncvar_def(x,
              units=units,
              dim=dim_time_wide,
              missval=miss,
              longname=sprintf("%s sensor-%s %s %s", measurand, sensor, cal, type),
              prec=types[[type]][['precision']]
    )
}
all_vars <- lapply(all_cols_ord, create_var)
obj_all <- nc_create("aqm388_allmeasurands.nc", all_vars)

# Add data
add_data <- function(i) {
    ncvar_put(obj_all, all_vars[[i]], df_wide[[all_cols_ord[[i]]]])
}
for (i in seq_along(all_cols_ord)) {
    ncvar_put(obj_all, all_vars[[i]], df_wide[[all_cols_ord[[i]]]])
}
lapply(seq_along(all_cols_ord), add_data)

# Add attributes for flags
flag_cols <- which(grepl("flag_", all_cols_ord))
flagreason_cols <- which(grepl("flagreason_", all_cols_ord))
tmp <- lapply(flag_cols, function(index) {
    ncatt_put(obj_all, all_vars[[index]], attname="long_name", sprintf("%s severity", all_vars[[index]]))
    ncatt_put(obj_all, all_vars[[index]], attname="flag_values", attval=paste(seq_along(flag_categories), collapse=' '))
    ncatt_put(obj_all, all_vars[[index]], attname="flag_meanings", attval=paste(flag_categories, collapse=' '))
})

tmp <- lapply(flagreason_cols, function(index) {
    ncatt_put(obj_all, all_vars[[index]], attname="long_name", sprintf("%s reason", all_vars[[index]]))
    ncatt_put(obj_all, all_vars[[index]], attname="flag_values", attval=paste(seq_along(flagreason_categories), collapse=' '))
    ncatt_put(obj_all, all_vars[[index]], attname="flag_meanings", attval=paste(flagreason_categories, collapse=' '))
})

# Save to disk!
nc_close(obj_all)

# Try opening with tidync
src_all <- tidync("aqm388_allmeasurands.nc")
nc_df_all <- src_all |> hyper_tibble() |>
            mutate(time=as_datetime(time))
# Can't seem to automatically get cal name due to how the character name is stored!
nc_df_all

# Calibration version - awkward to read
tidync("aqm388_allmeasurands.nc") |>
    hyper_tibble() |>
    mutate(time = as_datetime(time)) |>
    tail()

# Check meta looks good
ncmeta::nc_atts("aqm388_allmeasurands.nc") |>
    filter(name != '_FillValue') |>
    unnest(cols=c(value)) |> 
    print(n=Inf)

########################################################################################
# Should really use standard names!: 
# http://cfconventions.org/Data/cf-standard-names/current/src/cf-standard-name-table.xml
#long <- sprintf("%s for the %d%s %s sensor under calibration version '%s'",
#                )
# Add description of flags! Or should flags just have a status_flag column and no info level?
# add ancilliary_variables to each measurement
# See examples H2.4 onwards
# https://cfconventions.org/cf-conventions/cf-conventions.html#flags
# So for MAQS data, Nathan uses 'mass_fraction_of_<species_in_air'
ncmeta::nc_att("~/Downloads/maqs-Teledyne-T500U_maqs_202301_NO2-concentration_Unratified_v2.2.nc",
               "mass_fraction_of_nitrogen_dioxide_in_air",
               "chemical_species") |>
    unnest(cols=c(value))
# And has type, dimension, practical_units, standard_name, long_name, valid_min, valid_max, call_methods,
# coordinates, chemical_species
# Long name is Mole Fraction of <species> in air
# No idea what call_methods are
# valid_min and valid_max look like the min and max values from the data
# coordinates are locations of the site, whicH I could do too
ncmeta::nc_atts("~/Downloads/maqs-Teledyne-T500U_maqs_202301_NO2-concentration_Unratified_v2.2.nc",
               "mass_fraction_of_nitrogen_dioxide_in_air") |>
    mutate(value = as.character(value))

# For PM Units are ug  m-3
ncmeta::nc_atts("~/Downloads/maqs-fidas-1_maqs_202204_PM-concentration_Ratified_v2.1.nc",
               "mass_concentration_of_pm2p5_ambient_aerosol_in_air") |>
    mutate(value = as.character(value))

# For Temp we have Kelvin and for RH we have %
ncmeta::nc_atts("~/Downloads/maqs-fidas-1_maqs_202203_surface-met_Ratified_v2.1.nc",
               "air_temperature") |>
    mutate(value = as.character(value))
ncmeta::nc_atts("~/Downloads/maqs-fidas-1_maqs_202203_surface-met_Ratified_v2.1.nc",
               "relative_humidity") |>
    mutate(value = as.character(value))
ncmeta::nc_atts("~/Downloads/maqs-fidas-1_maqs_202203_surface-met_Ratified_v2.1.nc",
               "air_pressure") |>
    mutate(value = as.character(value))
# Global vars
ncmeta::nc_atts("~/Downloads/maqs-fidas-1_maqs_202203_surface-met_Ratified_v2.1.nc",
               "NC_GLOBAL") |>
    mutate(value = as.character(value)) |>
    print(n=Inf)

# MAQS USES for gas (well NO2 at least):
#   - Fill value = -1e20
#   - type = float32
#   - practical_units = ppb
#   - standard_name = mass_fraction_of_nitrogen_dioxide_in_air
#   - long_name = Mole Fraction of Nitrogen Dioxide in air
#   - valid_min = 0.624 (from data!)
#   - valid_max = 73.7 (from data!)
#   - call_methods = time:mean
#   - coordinates = lat lon
#   - chemical_species = NO2
# What about PM?
# And Temp and RH?

tidync("~/Downloads/maqs-Teledyne-T500U_maqs_202301_NO2-concentration_Unratified_v2.2.nc") |>
    hyper_tibble() |>
    summarise(min(mass_fraction_of_nitrogen_dioxide_in_air),
              max(mass_fraction_of_nitrogen_dioxide_in_air)
              )


##############################################################################
# Final version!
##############################################################################
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
    "RelHumidity", "relative_humidity", "relative_humidity", "Relative Humidity", "%", ""
    )
    
export_to_netcdf <- function(in_instrument, in_measurand, in_location, version=1) {
    # Get data from DB 
    # TODO update to lcs when finished debugging
    df <- tbl(con, "lcs_hourly") |> 
            filter(instrument == in_instrument,
                   measurand %in% in_measurand,
                   location == in_location) |> 
            collect()
    
    # Get metadata
    company_meta <- tbl(con, "lcsinstrument") |>
        filter(instrument == in_instrument) |>
        inner_join(tbl(con, "lcscompany"), by="company") |>
        collect()
    
    first_date <- as_date(min(df$time))
    end_date <- as_date(max(df$time))
    
    measurand_title <- ifelse(length(in_measurand == 1), 
                              in_measurand,
                              ifelse(all(grepl("PM", in_mesaurand)), "PM",
                                     ifelse(sort(measurand) == c('RelHumidity', 'Temperature'),
                                            'Met',
                                            'ERROR')))
    
    filename <- sprintf("%s_%s_%s_%s_%s_%s.nc",
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
       ) |>
        mutate(
            flag_char = ifelse(is.na(flag_char), 'None', flag_char),
            flag_char = gsub(",.+", "", flag_char),
            flagreason_char = ifelse(is.na(flagreason_char), 'NA', flagreason_char),
       )
    
    df_wide <- df |>
        pivot_wider(names_from=measurand, values_from=c(measurement, flag_char, flagreason_char))
    
    # Convert characters to enums
    # Don't have hardcoded order for flag reasons, but just ensure that NA is at the start
    flagreason_categories <- unique(df$flagreason_char)
    flagreason_categories <- c('NA', setdiff(flagreason_categories, 'NA'))
    
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
                  longname=meta$long_name,
                  prec="double"
        )
    })
    
    flag_vars <- lapply(measurands, function(x) {
        meta <- measurand_netcdf_metadata |> filter(measurand == x)
        ncvar_def(sprintf("qc_flag_level_%s", meta$ncdf_name),
                          units="",
                          dim=dim_time,
                          longname=sprintf("%s Flag Level", x),
                          prec="short"
        )
    })
    
    flagreason_vars <- lapply(measurands, function(x) {
        meta <- measurand_netcdf_metadata |> filter(measurand == x)
        ncvar_def(sprintf("qc_flag_%s", meta$ncdf_name),
                          units="",
                          dim=dim_time,
                          longname=sprintf("%s QC Flag", x),
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
                        pull(dateapplied)
    first_sensor_measurements <- df_wide |> 
                                    group_by(sensornumber) |>
                                    summarise(
                                        earliest=  min(time, na.rm=T),
                                        latest= max(time, na.rm=T)
                                    ) |>
                                    ungroup() |>
                                    arrange(sensornumber) 
    ncatt_put(obj, var_cal, attname="date_calibration_applied", attval=paste(cals_applied, collapse=' '))
    ncatt_put(obj, var_sensornumber, attname="date_first_measurement", attval=paste(first_sensor_measurements$earliest, collapse=' '))
    ncatt_put(obj, var_sensornumber, attname="date_last_measurement", attval=paste(first_sensor_measurements$latest, collapse=' '))
    
    # add metadata for measurements and flags per measurement
    result <- lapply(measurands, function(x) {
        meta <- measurand_netcdf_metadata |> filter(measurand == x)
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
                  attval=min(df_wide[[sprintf("measurement_%s", x)]], na.rm=T)
        )
        ncatt_put(obj, 
                  measurement_vars[[x]], 
                  attname="coordinates", 
                  attval=sprintf("%fN %fE", locations[[in_location]]$lat, locations[[in_location]]$lon)
        )
        ncatt_put(obj, 
                  measurement_vars[[x]], 
                  attname="valid_max", 
                  attval=max(df_wide[[sprintf("measurement_%s", x)]], na.rm=T)
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
        ncatt_put(obj, flagreason_vars[[x]], attname="flag_meanings", attval=paste(flagreason_categories, collapse=' '))
        
        # ancillary variables to explicitly link measurements and flags
        ncatt_put(obj, 
                  measurement_vars[[x]], 
                  attname="ancilliary_variables", 
                  attval=sprintf("qc_flag_%s qc_flag_level_%s", meta$ncdf_name, meta$ncdf_name)
        )
    })
    
    # Global metadata
    global_meta <- list(
        Conventions="CF-1.6",
        title=ifelse(length(in_measurand == 1), 
                     sprintf("%s Concentration", in_measurand),
                     ifelse(all(grepl("PM", in_mesaurand)), "Particulate Matter Concentration",
                            ifelse(sort(measurand) == c('RelHumidity', 'Temperature'),
                                   'Surface Meteorology',
                                   'ERROR'))),
        institution="Wolfson Atmospheric Chemistry Laboratories, University of York",
        source=sprintf("Surface observations of %s from a %s low-cost instrument", 
                       in_measurand, company_meta$companyhumanreadable),
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
        time_coverage_start=min(df_wide$time, na.rm=T),
        time_coverage_end=max(df_wide$time, na.rm=T),
        last_revised_date=lubridate::now(tzone="UTC"),
        licence="Data usage licence - UK Government Open Licence agreement: http://www.nationalarchives.gov.uk/doc/open-government-licence",
        acknowledgement="Acknowledgement of NERC and QUANT as the data providers is required whenever and wherever this data is used",
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

export_to_netcdf("AQM388", "NO2", "Manchester")
export_to_netcdf("AQM388", c("PM1", "PM2.5", "PM10"), "Manchester")

tidync("AQM388_NO2_Manchester_2019-12-18_2022-10-31.nc") |>
    hyper_tibble()
ncmeta::nc_atts("AQM388_NO2_Manchester_2019-12-18_2022-10-31.nc") |>
    mutate(value = as.character(value)) |>
    print(n=Inf)

tidync("AQM388_PM1-PM2.5-PM10_Manchester_2019-12-16_2022-10-31.nc") |>
    hyper_tibble()
ncmeta::nc_atts("AQM388_PM1-PM2.5-PM10_Manchester_2019-12-16_2022-10-31.nc") |>
    mutate(value = as.character(value)) |>
    print(n=Inf)

ncmeta::nc_atts("~/Downloads/maqs-fidas-1_maqs_202204_PM-concentration_Ratified_v2.1.nc") |>
    mutate(value = as.character(value)) |>
    print(n=Inf)
