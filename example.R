#
#    example.R
#    ~~~~~~~~~
#
#   Loads QUANT Clean data from all devices, converts it into a format suitable
#   for analysis and demonstrates basic use cases.

library(tidyverse)

source("load_data.R")

quant_folder <- "/home/stuart/Documents/quant_data/clean/"
df <- load_data(quant_folder, companies=c("Aeroqual", "AQMesh"),
                start="2020-01-01", end="2020-04-28")
df

# Filter data to just AQMesh devices and recorded between 2 dates
aqmesh_march <- df %>%
                    filter(manufacturer == 'AQMesh',
                           timestamp > '2020-03-17',
                           timestamp < '2020-03-20')
# Plot the NO2 time-series from these 3 days with each device
# having its own colour
aqmesh_march %>%
    ggplot(aes(x=timestamp, y=NO2, colour=device)) +
        geom_line()

# Calculate proportion of missingness by group; tote that this gives a
# different result to the Python version.
# This is because the Python load_data() resampling method will create rows
# for all possible minutes. However, the resampling method in the R
# implementation doesn't, and so it doesn't identify that AQMesh are missing
# lots of data
df %>%
    group_by(device) %>%
    summarise(mean(is.na(NO2)))
