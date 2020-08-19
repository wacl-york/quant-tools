# QUANTtools

Tools for working with the QUANT data, including example code for loading the data into Python and basic analysis.

# Installation 

Download the `load_data.py` file and place it in your working directory, or somewhere where it can be imported.

# Example

Firstly load the provided `load_data` function.

```python
from load_data import load_data
```

`load_data` will also import `pandas`, so it doesn't need to be explicitly loaded now.
Also load `seaborn` and `matplotlib` for plotting.

```python
import seaborn as sns
import matplotlib.pyplot as plt
```

The `QUANT/Data/Clean` GoogleDrive folder needs to be available locally, either through Google Drive sync or having been manually downloaded.
Set the `quant_folder` variable to this location, then the `load_data` function can be used.
Calling `load_data(quant_folder)` loads all the CSV files in this folder into a single `pandas` data frame.
However, there are options to load only subsets, such as which companies to load and specifying a timeframe.

The `companies` argument accepts a list with values from the 4 companies included in the study:

  - `Aeroqual`
  - `AQMesh`
  - `Zephyr`
  - `QuantAQ`

The `start` and `end` arguments accept a date in `YYYY-mm-dd` format indicating the earliest and latest (both inclusive) dates to load data from.

The example below will load data from just Aeroqual and AQMesh devices, in the inclusive period 2020-01-01 to 2020-04-28.

```python
quant_folder = "/path/to/folder/Clean"
df = load_data(quant_folder, companies=["Aeroqual", "AQMesh"],
                 start="2020-01-01", end="2020-04-28")
df
```

```
                  timestamp manufacturer   device    CO2   NO   NO2    O3  RelHumidity  Temperature
0       2020-01-01 00:00:00       AQMesh   AQM388  419.0 -8.2   6.2  40.5         92.0         7.40
1       2020-01-01 00:00:00       AQMesh   AQM389  448.0 -5.3   4.7  40.6         94.4         7.50
2       2020-01-01 00:00:00       AQMesh   AQM391  424.0 -3.9  10.4  35.0         92.7         7.30
3       2020-01-01 00:00:00       AQMesh   AQM801  430.0 -7.0  11.5   0.0         82.7         7.60
4       2020-01-01 00:00:00     Aeroqual  AQY436A    NaN  NaN  -2.2  35.1         90.6         7.39
...                     ...          ...      ...    ...  ...   ...   ...          ...          ...
1474743 2020-04-28 23:59:00       AQMesh   AQM390    NaN  NaN   NaN   NaN          NaN          NaN
1474744 2020-04-28 23:59:00       AQMesh   AQM391    NaN  NaN   NaN   NaN          NaN          NaN
1474745 2020-04-28 23:59:00     Aeroqual   AQY872    NaN  NaN  -6.2  33.2         99.9         6.72
1474746 2020-04-28 23:59:00     Aeroqual  AQY873A    NaN  NaN  -9.8  39.3         99.9         6.37
1474747 2020-04-28 23:59:00     Aeroqual   AQY874    NaN  NaN -10.5  36.1         95.4         8.11
```

Other options to `load_data` specify what resampling - if any - should be used, and which pollutants to include.
Resampling is controlled by the `resample` argument, which by default is `1Min`.
This value can be changed to any value accepted by [`pandas.resample`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.resample.html), or set to `None` if no resampling is desired.

By default, the following measurands are included in the final data frame:

  - NO
  - NO2
  - O3
  - CO
  - CO2
  - Temperature
  - Relative humidity

This is specified by the default value to the `subset` argument, although it can be overwritten to select particular pollutants of interest: `load_data(quant_folder, subset=['NO2', 'O3'])`.
To see the full range of possible values, set `subset=None` to return all possible columns.

Once the data frame has been loaded, it can be queried using [`query`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.query.html) to extract specific rows using human readable strings.
For example, the code snippet below shows how to specify a specific manufacturer and a time-frame.
NO2 time-series from this period are then plotted, using the `device` column to colour each device's line.

```
aqmesh_march = df.query("(manufacturer == 'AQMesh') & (timestamp > '2020-03-17') & (timestamp < '2020-03-20')")
plt.figure(figsize=(15,10))
sns.lineplot(data=aqmesh_march, x='timestamp', y='NO2', hue='device');
plt.show()
```

![AQMesh line plot](figures/aqmesh_line.png)

Having the data in this format makes it easy to run basic analyses, such as determining the proportion of missing observations of a particular measurement by device.

The code snippet below calculates the proportion of NO2 missingness for our AQMesh and Aeroqual dataset recorded between 2020-01-01 to 2020-04-28; highlight that our 4 AQM devices all have > 60% missingness. 
This can be explained by the fact that the data is resampled to 1 minute averages, but AQMesh units were recording at 15 min frequencies for much of this period.

This one-line call chains together several operations:

  - [`isnull`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.isnull.html) returns a copy of the data frame with each value replaced by a boolean `True` or `False` indicating whether that value was missing
  - the [`set_index`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.set_index.html) call on the grouping column is required to not include this column in the `isnull` check - we aren't interested in whether a device ID was missing as it should always be present
  - `['NO2']` subsets to just the NO2 column, along with the `device` index
  - [`groupby`](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.groupby.html) groups the data frame by each unique `device` ID
  - `mean()` calculates the mean of each `NO2` column per `device` group. In Python, `True` is equivalent to 1, while `False=0`. The mean is therefore calculating the proportion of NO2 missing values that are `True`, i.e. the mean of `[1, 0, 1]` is 0.67.


```python
df.set_index('device').isnull()['NO2'].groupby('device').mean()
```

```
device
AQM173     0.458051
AQM388     0.610810
AQM389     0.628405
AQM390     0.599464
AQM391     0.672171
AQM801     0.489521
AQY436A    0.231568
AQY872     0.003491
AQY873A    0.861775
AQY874     0.002497
AQY875     0.001261
Name: NO2, dtype: float64
```

# Extending this functionality

Both the `load_data.py` file and the examples shown here are not intended to be the definitive way of using the QUANT data, but instead are merely examples of one way this can be achieved.
Feel free to extend the `load_data.py` function to better fit your own needs and workflow.

The `load_data` function is fairly simple, only opening files and running very basic pre-processing steps (resampling, subsetting columns).
However, one thing to bear in mind is that it is also renaming some columns related to temperature and humidity to ensure that the `Temperature` and `RelHumidity` columns are measuring the same quantity across companies.
In particular, it renames the following measurands from the clean CSV files:

  - Zephyr: `Temperature` becomes `TempPCB`
  - Zephyr: `TempAmb` becomes `Temperature`
  - Zephyr: `RelHumidity` becomes `RelHumPCB`
  - Zephyr: `RelHumAmb` becomes `RelHumidity`
  - QuantAQ: `TempMan` becomes `Temperature`
  - QuantAQ: `RelHumMan` becomes `RelHumidity`

The raw Zephyr data has 2 fields for temperature: `ambTempC` and `tempC`, which the scraper renames to `TempAmb` and `Temperature` respectively.
However, `tempC` actually measures the unit's temperature of the unit, so it has been renamed to `TempPCB`, while `ambTempC` is the value that we are interested in and has thus has been renamed to `Temperature` for consistency.
Likewise for relative humidity.

QuantAQ likewise have 2 temperature measurements: `temp_manifold` and `temp_box`, which the scraper renames to `TempMan` and `TempBox`. 
Similarily, we are interested in the manifold conditions so these column are renamed to `Temperature` and `RelHumidity`.

Ideally, this renaming would be applied in the scraper and retroactively applied to the full dataset.
However, this has not been done to ensure backwards compatibility, although it might be worth adding at some point in the future.

