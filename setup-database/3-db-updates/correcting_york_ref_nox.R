# Following discussions with Ricardo and Bureau Veritas, we've identified some errors
# in our NO and NO2 parsing from our data logger on the T200 at Fishergate.
# This script corrects them.
# 2023-02-23
library(tidyverse)
library(lubridate)
library(odbc)
library(openair)
library(patchwork)
library(ggpointdensity)
library(readxl)

con <- dbConnect(odbc(), "QUANT")

df_quant <- tbl(con, "ref_hourly") |>
                filter(location == 'York', measurand %in% c('NO', 'NO2')) |>
                select(time, measurand, quant=measurement) |>
                collect() 

# Also ratified until end of Sept 2022
df_aurn <- importAURN(site="YK11", year=2020:2022, pollutant=c("no", "no2")) |>
            select(time=date, no, no2) |>
            pivot_longer(-time, names_to='measurand', values_to='aurn') |>
            mutate(measurand = toupper(measurand))

comb <- df_quant |>
            inner_join(df_aurn, by=c("time", "measurand")) |>
            pivot_longer(c(quant, aurn)) |>
            pivot_wider(names_from=measurand, values_from=value) |>
            mutate(NOx = NO + NO2) |>
            pivot_longer(c(NO, NO2, NOx), names_to="measurand") |>
            mutate(measurand = factor(measurand, levels=c("NOx", "NO", "NO2")))

plot_data <- function(df) {
    p1 <- df |>
        mutate(name = toupper(name)) |>
        filter(time < "2021-01-01") |>
        ggplot(aes(x=time, y=value, colour=name)) +
            geom_line(alpha=0.7) +
            scale_colour_brewer("", palette="Set1") +
            theme_bw() +
            facet_wrap(~measurand, scales="free_y") +
            labs(x="", y="NO/NO2 (ppb)", title="Difference between our NOx data and that available through the AURN",
                 subtitle="QUANT data is the 1 min logged data averaged to hourly, AURN is hourly data as provided by the AURN") +
            theme(legend.position = c(0.06, 0.93),
                  legend.background = element_rect(fill=NA))
    
    p2 <- df |>
        pivot_wider() |>
        filter(time < "2021-01-01") |>
        ggplot(aes(x=quant, y=aurn)) +
            geom_pointdensity(na.rm=T) +
            stat_smooth(colour="red") +
            theme_bw() +
            ggpubr::stat_regline_equation() +
            facet_wrap(~measurand, scales="free") +
            scale_colour_viridis_c() +
            guides(colour="none") +
            labs(x="QUANT", y="AURN") +
            theme(legend.position = c(0.06, 0.95),
                  legend.background = element_rect(fill=NA))
    
    p1 / p2
}

plot_data(comb)


# Ok what if swap NO and NO2?
# Looks a lot better!
comb_swap <- comb |>
            mutate(measurand = as.character(measurand),
                   measurand = ifelse(name == 'quant', gsub("^NO$", "NO_old", measurand), measurand),
                   measurand = ifelse(name == 'quant', gsub("^NO2$", "NO", measurand), measurand),
                   measurand = ifelse(name == 'quant', gsub("^NO_old$", "NO2", measurand), measurand))
plot_data(comb_swap)

# Now to apply the cal factors
cal_fn <- "~/GoogleDrive/WACL/BOCS/Calibration/Analyses/data/Reference/York/NOx/raw/York Fishergate_Calibrations_2020_2022.xlsx"
# Have confirmed the 2 time columns are identical so can just use one
cals <- readxl::read_xlsx(cal_fn) |>
            select(date = Date...2, NO_zero = Zero...3, NO_sens = Sensitivity...4, 
                   NOx_zero = Zero...8, NOx_sens = Sensitivity...9) |>
            mutate(date = as_date(date)) |>  # This is a date, not datetime
            pivot_longer(-date, names_sep="_", names_to=c("measurand", "field")) |>
            pivot_wider(names_from="field", values_from="value")
comb_cal <- comb_swap |>
    mutate(date = as_date(time)) |>
    left_join(cals, by=c("date", "measurand")) |>
    mutate(
        zero = ifelse(is.na(zero), 0, zero),
        sens = ifelse(is.na(sens), 1, sens),
        value = ifelse(name == 'quant',
                          zero + sens * value,
                          value))
plot_data(comb_cal)

# Still definitely missing some factors!
# What if the raw data itself is wrong?

# Let's read it on from the DB assuming the 


######### TODO DEBUGGING TESTING!
# LAB NOTES FROM 2023-02-24: 
# CAN'T CHOOSE WHAT TO PUT ON THE ANALOG OUTS
# https://www.teledyne-api.com/prod/Downloads/T200%20%26%20T200U%20NVS%20Manual%20-%20083730200.pdf
# manual is for a different version where it lets you select what gets put on analogue puts
# So just assume it's NOx/NO/NO2
# we have it setup to autocal in COZI - what is it doing in Fishergate?
# Want to test analog outputs but can't access screw terminals

# REMOVE CALS! NOx cals are easy
df_no2$NOx[(hour(df_no2$time) == 0 & minute(df_no2$time) >= 44) | (hour(df_no2$time) == 1 & minute(df_no2$time) <= 14)] <- NA
df_no2$NO2_raw[(hour(df_no2$time) == 0 & minute(df_no2$time) >= 44) | (hour(df_no2$time) == 1 & minute(df_no2$time) <= 14)] <- NA
# Remove spikes that have identified and flagged in DB
# NB: these have been labelled as NO2 but actually are NO
# Apply these to all fields
visit_times <- tbl(con, "flag") |>
    filter(instrument == 'Ref_York_NO2', measurand == 'NO2') |>
    select(time) |>
    mutate(visit=1) |>
    collect()
df_no2 <- df_no2 |>
    left_join(visit_times, by="time") |>
    mutate(NOx = ifelse(!is.na(visit), NA, NOx),
           NO = ifelse(!is.na(visit), NA, NO),
           NO2_raw = ifelse(!is.na(visit), NA, NO2_raw)
           ) |>
    select(-visit)

# Now to apply the cal factors
cal_fn <- "~/GoogleDrive/WACL/BOCS/Calibration/Analyses/data/Reference/York/NOx/raw/York Fishergate_Calibrations_2020_2022.xlsx"
# Have confirmed the 2 time columns are identical so can just use one
cals <- readxl::read_xlsx(cal_fn) |>
            select(date = Date...2, NO_zero = Zero...3, NO_sens = Sensitivity...4, 
                   NOx_zero = Zero...8, NOx_sens = Sensitivity...9) |>
            mutate(date = as_date(date)) |>  # This is a date, not datetime
            pivot_longer(-date, names_sep="_", names_to=c("measurand", "field")) |>
            pivot_wider(names_from="field", values_from="value") %>%
            rbind(. |> filter(measurand == 'NOx') |> mutate(measurand = 'NO2_raw'))
# Add dummy cals for NO2 based on the column name used by Brian

# Make hourly dataset and add cal factors
df_no2_hourly <- df_no2 |>
                    pivot_longer(-time, names_to = "measurand") |>
                    mutate(date = as_date(time)) |>
                    inner_join(cals, by=c("date", "measurand")) |>
                    mutate(
                        value_cal = value * sens - zero,
                        time = floor_date(time, "1 hour")
                           ) |>
                    group_by(time, measurand)  |>
                    summarise(value = mean(value, na.rm=T),
                              value_cal = mean(value_cal, na.rm=T)) |>
                    ungroup() 

# Add AURN reference measurements
library(openair)
library(ggpointdensity)
library(patchwork)
df_aurn <- importAURN(site="YK11", year=2020:2022, pollutant=c("no", "no2")) |>
            select(time=date, NO=no, NO2_raw=no2) |>
            mutate(NOx = NO + NO2_raw,
                   NO2_manual=NO2_raw) |>
            pivot_longer(-time, names_to='measurand', values_to='aurn')


# Add manual NO2 cal as NOx - NO
df_uncal <- df_no2_hourly |> 
    select(time, measurand, value) |>
    pivot_wider(names_from=measurand, values_from=value) |>
    mutate(NO2_manual = NOx - NO) |>
    pivot_longer(-time, names_to="measurand", values_to="quant") |>
    inner_join(df_aurn, by=c("time", "measurand")) |> 
    pivot_longer(c(aurn, quant), names_to="source")
df_cal <- df_no2_hourly |> 
    select(time, measurand, value_cal) |>
    pivot_wider(names_from=measurand, values_from=value_cal) |>
    mutate(NO2_manual = NOx - NO) |>
    pivot_longer(-time, names_to="measurand", values_to="quant") |>
    inner_join(df_aurn, by=c("time", "measurand")) |> 
    pivot_longer(c(aurn, quant), names_to="source")

# Plot comparison
plot_data <- function(df, title="Difference between our NOx data and that available through the AURN") {
    p1 <- df |>
        mutate(source = toupper(source)) |>
        filter(time < "2022-01-01") |>
        ggplot(aes(x=time, y=value, colour=source)) +
            geom_line(alpha=0.7) +
            scale_colour_brewer("", palette="Set1") +
            theme_bw() +
            facet_wrap(~measurand, scales="free_y") +
            labs(x="", y="NO/NO2 (ppb)", title=title,
                 subtitle="QUANT data is the 1 min logged data averaged to hourly, AURN is hourly data as provided by the AURN") +
            theme(legend.position = c(0.06, 0.93),
                  legend.background = element_rect(fill=NA))
    rmses <- df |>
        pivot_wider(names_from="source") |>
        filter(time < "2022-01-01") |>
        group_by(measurand) |>
        summarise(rmse = sqrt(mean((aurn - quant)**2, na.rm=T)),
                  x_val=0,
                  y_val = 0.70 * max(aurn, na.rm=T),
                  lab = sprintf("RMSE = %.2f", rmse))
    
    p2 <- df |>
        pivot_wider(names_from="source") |>
        filter(time < "2022-01-01") |>
        ggplot(aes(x=quant, y=aurn)) +
            geom_pointdensity(na.rm=T) +
            stat_smooth(colour="red") +
            geom_text(aes(x=x_val, y=y_val, label=lab), data=rmses, hjust=0) +
            theme_bw() +
            ggpubr::stat_regline_equation() +
            facet_wrap(~measurand, scales="free") +
            scale_colour_viridis_c() +
            guides(colour="none") +
            labs(x="QUANT", y="AURN") +
            theme(legend.position = c(0.06, 0.95),
                  legend.background = element_rect(fill=NA))
    
    p1 / p2
}

plot_data(df_uncal)
plot_data(df_cal)            # mx + c

# What if I correct the NOx and NO by AURN and recalculate NO2?
df_cor <- df_uncal |>
    pivot_wider(names_from="source") |>
    group_by(measurand) |>
    nest() |>
    mutate(mod = map(data, function(x) lm(aurn ~ quant, x)),
           quant=map(mod, function(x) x$fitted),
           aurn=map(mod, function(x) x$model$aurn),
           time=map(data, function(x) x$time[complete.cases(x)])
           ) |>
    unnest(cols=c(quant, aurn, time)) |>
    select(time, measurand, quant, aurn) |>
    pivot_longer(c(quant, aurn), names_to="source")
df_calcor <- df_cal |>
    pivot_wider(names_from="source") |>
    group_by(measurand) |>
    nest() |>
    mutate(mod = map(data, function(x) lm(aurn ~ quant, x)),
           quant=map(mod, function(x) x$fitted),
           aurn=map(mod, function(x) x$model$aurn),
           time=map(data, function(x) x$time[complete.cases(x)])
           ) |>
    unnest(cols=c(quant, aurn, time)) |>
    select(time, measurand, quant, aurn) |>
    pivot_longer(c(quant, aurn), names_to="source")

plot_data(df_cor)
plot_data(df_calcor)

map_dfr(
    list(
        "Raw"=df_uncal,
        "Calibrated"=df_cal,
        "Corrected"=df_cor,
        "Calibrated + corrected"=df_calcor
    ), function(x) {
        x |>
            pivot_wider(names_from="source") |>
            group_by(measurand) |>
            summarise(rmse = sqrt(mean((aurn-quant)**2, na.rm=T))) |>
            ungroup()
    }, .id="Dataset"
) |>
    pivot_wider(names_from="measurand", values_from="rmse")

plot_data(df_uncal |> filter(measurand != 'NO2_raw') |> mutate(measurand = gsub("NO2_manual", "NO2", measurand)),
          title="Uncalibrated")
plot_data(df_cal |> filter(measurand != 'NO2_raw') |> mutate(measurand = gsub("NO2_manual", "NO2", measurand)),
          title="Calibrated")
plot_data(df_calcor |> filter(measurand != 'NO2_raw') |> mutate(measurand = gsub("NO2_manual", "NO2", measurand)),
          title="Linearly corrected using AURN measurements")

# NEXT IDEA!
# Looking at the spreadsheet I produced for Brian, it looked like
# it could be simply mislabelled, i.e. that the first NOx column was actually NO2,
# and the column labelled NO2 was actually NO
# But I investigated this and it's definitely not the case, and it only
# looked like this in the spreadsheet for one particular hour I was using as
# my test case

# Think the way we have the cal setup is wrong!
# Look at 5.9.2 in Cozi lab, think this will show it's incorrect in Fishergate

########## END DEBUGGING