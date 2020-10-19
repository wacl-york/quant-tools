"""

    load_data.py
    ~~~~~~~~~~~~

    Tools for loading QUANT data into pandas.

"""
import os
import glob
from datetime import datetime
import pandas as pd
from functools import partial
from multiprocessing import Pool


def load_data(folder, companies=None, start=None, end=None, resample="1Min",
              subset=['NO', 'NO2', 'O3', 'CO2', 'CO', 'Temperature',
                      'RelHumidity'],
              num_cpus=-1):
    """
    Loads QUANT data from multiple files into a single pandas data frame.

    Args:
        - folder (str): The folder containing all the CSV files. This should be
            a mirror of the QUANT/Data/Clean GoogleDrive folder.
        - companies (list): A list of companies to load data from. Can contain
        values: 'Aeroqual', 'AQMesh', 'Zephyr', 'QuantAQ'.
        - start (str): The earliest date to include data from, in YYYY-mm-dd
            format. If not provided then uses the earliest available date.
        - end (str): The latest date to include data from, in YYYY-mm-dd
            format. If not provided then uses the latest available date.
        - resample (str): Resampling format to use as specified by
            pandas.resample. If None then doesn't do any resampling.
        - subset (list): A list of pollutants to include in the final data
            frame. If None then returns all.
        - num_cpus (int): The number of cpus to use when reading the individual
            files. If < 1, then the maximum number of cpus is obtained from
            os.cpu_count().

    Returns:
        A pandas data frame with 1 row per observation per device, resampled to the
        specified frequency. There are manufacturer and device columns to index
        where the recording was made, and there are measurement columns for each
        pollutant of interest.
    """
    # Add trailing slash if not present
    folder = os.path.join(folder, '')

    # Find all files from the selected companies
    if companies is None:
        fns = glob.glob("{}*.csv".format(folder))
    elif isinstance(companies, str):
        fns = glob.glob("{}{}*.csv".format(folder, companies))
    else:
        fns = [f for company in companies for f in glob.glob("{}{}*.csv".format(folder, company))]

    # Subset to between the dates of interest (inclusive)
    if start is not None:
        try:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
        except ValueError:
            print("Error: cannot parse {} as a date in the format YYYY-mm-dd".format(start))
            return None
        fns = [ fn for fn in fns if get_date_from_filename(fn) is not None and get_date_from_filename(fn) >= start_dt ]

    if end is not None:
        try:
            end_dt = datetime.strptime(end, "%Y-%m-%d")
        except ValueError:
            print("Error: cannot parse {} as a date in the format YYYY-mm-dd".format(end))
            return None
        fns = [ fn for fn in fns if get_date_from_filename(fn) is not None and get_date_from_filename(fn) <= end_dt ]

    if num_cpus <= 0:
        num_cpus = os.cpu_count()

    # Load all files into a list of DataFrames
    # From docs:
    # Note that map may cause high memory usage for very long iterables.
    # Consider using imap() or imap_unordered() with explicit chunksize option for better efficiency.
    with Pool(processes=num_cpus) as process_pool:
        dfs = process_pool.map(partial(load_file, resample=resample), fns)

    # Combine all the individual data frames into a single data frame
    if len(dfs) == 0:
        print("No data found for the specified parameters.")
        return None

    df_combined = pd.concat(dfs, ignore_index=True)

    # Rename the unuseful Temperature label for Zephyr to TempPCB to be explicit in what it measures
    df_combined.loc[(df_combined['measurand'] == 'Temperature') & (df_combined['manufacturer'] == 'Zephyr'), 'measurand'] = 'TempPCB'
    # Rename the useful TempAmb label to Temperature so that it's consistent with AQMesh & Aeroqual
    df_combined.loc[(df_combined['measurand'] == 'TempAmb') & (df_combined['manufacturer'] == 'Zephyr'), 'measurand'] = 'Temperature'
    # Likewise for relative humidity
    df_combined.loc[(df_combined['measurand'] == 'RelHumidity') & (df_combined['manufacturer'] == 'Zephyr'), 'measurand'] = 'RelHumPCB'
    df_combined.loc[(df_combined['measurand'] == 'RelHumAmb') & (df_combined['manufacturer'] == 'Zephyr'), 'measurand'] = 'RelHumidity'

    # QuantAQ renaming
    df_combined.loc[(df_combined['measurand'] == 'TempMan') & (df_combined['manufacturer'] == 'QuantAQ'), 'measurand'] = 'Temperature'
    df_combined.loc[(df_combined['measurand'] == 'RelHumMan') & (df_combined['manufacturer'] == 'QuantAQ'), 'measurand'] = 'RelHumidity'

    # Rename PurpleAir columns. This wasn't done for the initial download but
    # has been for every download since
    df_combined.loc[(df_combined['measurand'] == 'pm1_0_atm') & (df_combined['manufacturer'] == 'PurpleAir'), 'measurand'] = 'PM1'
    df_combined.loc[(df_combined['measurand'] == 'pm2_5_atm') & (df_combined['manufacturer'] == 'PurpleAir'), 'measurand'] = 'PM2.5'
    df_combined.loc[(df_combined['measurand'] == 'pm10_0_atm') & (df_combined['manufacturer'] == 'PurpleAir'), 'measurand'] = 'PM10'
    # And the OPC-B measurements too
    df_combined.loc[(df_combined['measurand'] == 'pm1_0_atm_b') & (df_combined['manufacturer'] == 'PurpleAir'), 'measurand'] = 'PM1_b'
    df_combined.loc[(df_combined['measurand'] == 'pm2_5_atm_b') & (df_combined['manufacturer'] == 'PurpleAir'), 'measurand'] = 'PM2.5_b'
    df_combined.loc[(df_combined['measurand'] == 'pm10_0_atm_b') & (df_combined['manufacturer'] == 'PurpleAir'), 'measurand'] = 'PM10_b'

    df_combined.loc[(df_combined['measurand'] == 'current_humidity') & (df_combined['manufacturer'] == 'PurpleAir'), 'measurand'] = 'RelHumidity'
    df_combined.loc[(df_combined['measurand'] == 'current_temp_f') & (df_combined['manufacturer'] == 'PurpleAir'), 'measurand'] = 'Temperature_F'

    # Subset to specified pollutants
    if subset is not None:
        if isinstance(subset, str):
            subset = [subset]
        df_combined = df_combined[df_combined['measurand'].isin(subset)]

    # unstack() pivots the long to wide, although it requires indexes to be set first
    wide = df_combined.set_index(['timestamp', 'manufacturer', 'device', 'measurand']).unstack()
    # droplevel removes the hierarchical column names
    # reset index removes pandas index so that can index all columns easily
    wide = wide.droplevel(0, axis=1).reset_index()

    # Remove the 'measurands' column name
    wide.columns.name = None

    return wide

def load_file(filename, resample="1Min"):
    """
    Loads a single CSV file into Pandas.

    After reading the file into Pandas, duplicate rows are dropped and any
    required resampling is performed.

    Args:
        - filename (str): Filename to load. Must be a CSV.
        - resample (str): Resampling format to use as specified by
            pandas.resample. If None then doesn't do any resampling.

    Returns:
        A pandas dataframe in long format with 3 columns:
            - timestamp
            - measurand
            - value
    """
    # Read this CSV file into pandas
    df = pd.read_csv(filename,
                     dtype={'measurand': 'object', 'value': 'float64'},
                     parse_dates=[0],
                     infer_datetime_format=True)

    # Remove multiple measurements of the same pollutant at the same timestamp
    # There shouldn't be many, if at all, but I've found a few in 1 Zephyr file
    df.drop_duplicates(subset=['timestamp', 'measurand'], inplace=True,
                       keep=False)

    # Resample to 1 min average. Has 3 benefits: reduces Zephyyr data down from
    # ~10s to every minute, rounds every recording to the nearest whole minute,
    # and creates rows for every minute even if we didn't have any samples during that minute.
    # This last point helps to identify missingness.
    # To resample, need to convert to wide, resample, then back to long
    if resample is not None:
        wide = df.pivot(index="timestamp", columns="measurand", values="value")
        try:
            wide = wide.resample(resample).mean()
            df = wide.reset_index().melt(id_vars="timestamp")
        except ValueError:
            print("Cannot resample {} to '{}' frequency. Data will be in its original time resolution".format(filename, resample))

    # Extract manufacturer and device from filenames which are in the format
    # <manufacturer>_<device>_<date>.csv
    manufacturer, device, date = os.path.basename(filename).split("_")
    df['manufacturer'] = manufacturer
    df['device'] = device

    return df

def get_date_from_filename(filename):
    """
    Extracts the recording data from a QUANT filename.

    QUANT clean data files are named according to the convention
    "<manufacturer>_<device>_<date>.csv", where <date> is in YYYY-mm-dd format.

    This function extracts date and parses it as a datetime object.
    NB: this function returns a datetime object rather than date, despite
    not needing second precision.
    This is because datetime has a robust parsing method, while date's
    fromisoformat parser was only added in Python 3.7

    Args:
        - filename (str): The CSV filename.

    Returns:
        The date of recording as a datetime object.
    """
    # Remove directories to just get base filename
    base = os.path.basename(filename)
    # Remove file extension
    try:
        fn = os.path.splitext(base)[0]
    except IndexError:
        return None  # Shouldn't ever be reached

    # Extract date, assuming syntax <manufacturer>_<device>_<date>
    try:
        date_str = fn.split("_")[2]
    except IndexError:
        return None

    # Parse date from string
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None

    return dt
