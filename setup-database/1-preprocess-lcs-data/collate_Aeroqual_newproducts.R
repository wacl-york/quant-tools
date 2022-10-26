# collate_Aeroqual.R
# ~~~~~~~~~~~~~~~~~~
source("~/repos/quant-tools/load_data.R")
library(tidyverse)
library(odbc)

INPUT_DIR <- "~/Documents/quant_data/one-off-downloads/extracted/"

# Collates Aeroqual CSV files in the Clean format from multiple archives into 2 datasets:
#   - First calibration products
#   - Second calibration products

########################
# First calibration
########################
###########
# I'm happy then that the Clean folder contains OOB up until 23nd April, then 1st Cal after (in the usual col),
# And the 2nd cal back to 22nd April in a separate field
# I.e. if I just add in the retrospective manual cal from 18th Feb - 23rd April we'll have the full dataset.
df_clean <- load_data("~/Documents/quant_data/Clean", companies="Aeroqual",
                     subset=c('NO2_cal', 'O3_cal', 'PM10_cal', 'PM2.5 cal')) %>%
            select(-manufacturer) %>%
            pivot_longer(-c(timestamp, device), names_to="measurand")

# Prepare for insertion into DB
to_upload <- df_clean |>
    mutate(sensornumber = 1,
           calibrationname = 'new product',
           measurand = gsub('[_ ]cal', '', measurand)) |>
    select(instrument=device, measurand, sensornumber, calibrationname, time=timestamp, measurement=value) |>
    filter(!is.na(measurement))

# Upload straight away!
con <- dbConnect(odbc(), "QUANT")
batch_size <- 1e5
n_obs <- nrow(to_upload)
start_points <- seq(from=1, to=n_obs, by=batch_size)
i <- 1
for (start in start_points) {
    cat(sprintf("On batch %d/%d", i, length(start_points)))
    end <- min(n_obs, start + batch_size - 1)
    dbAppendTable(con, "measurement", to_upload[start:end, ])
    i <- i + 1
}
dbDisconnect(con)