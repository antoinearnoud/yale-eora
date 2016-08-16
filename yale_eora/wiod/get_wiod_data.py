# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 16:58:27 2015

@author: Antoine
"""
import pandas
import logging
import os
import pkg_resources


from yale_eora.config import Config
#from yale_eora.eora.get_eora_data import eora_df
#from yale_eora.eora.analyze_eora import aggregate_eora, df_aggregate_to_csv


log = logging.getLogger(__name__)

parser = Config(
    config_files_directory = os.path.join(pkg_resources.get_distribution('yale-eora').location)
    )

wiod_directory = parser.get('data', 'wiod_data')
wiod_output = parser.get('data', 'wiod_output')


# WIOD
# df_wiod_xls = pandas.read_excel(os.path.join(wiod_directory, 'aggregates_from_stata.xlsx'), header = 0)


def save_df_to_hdf(df, hdf_file_name, key):
    file_path = os.path.join(wiod_output, hdf_file_name)
    df.to_hdf(file_path, key)
    pandas.DataFrame().to_hdf


def wiod_parser(years = range(1995, 2012)):
    df_wiod_by_year = dict()
    df_wiod = pandas.read_stata(os.path.join(wiod_directory, 'wiot_full.dta'))
    for year in years:
        df_wiod_by_year[year] = df_wiod[df_wiod['year'] == year]
        save_df_to_hdf(df_wiod_by_year[year], 'wiod_data.hdf5', 'year_' + str(year))
    return df_wiod_by_year


def import_hdf_to_df(hdf_file_name, key):
    file_path = os.path.join(wiod_output, hdf_file_name)
    store = pandas.HDFStore(file_path)
    df = store[key]
    return df


def import_wiod_hdf(years):
    df_wiod_by_year = dict()
    for year in years:
        df_wiod_by_year[year] = import_hdf_to_df('wiod_data.hdf5',  'year_' + str(year))
    return df_wiod_by_year


def wiod_aggregate(df_wiod_by_year):
    df_wiod_aggregate_by_year = dict()
    for year, df in df_wiod_by_year.items():
        df.loc[df['col_item'] == 37, 'col_country'] = df['col_country'] + '_FD'
        df.loc[df['col_item'] == 38, 'col_country'] = df['col_country'] + '_FD'
        df.loc[df['col_item'] == 39, 'col_country'] = df['col_country'] + '_FD'
        df.loc[df['col_item'] == 41, 'col_country'] = df['col_country'] + '_FD'
        df.loc[df['col_item'] == 42, 'col_country'] = df['col_country'] + '_FD'
        df_wiod_aggregate_by_year[year] = df.groupby(['row_country', 'col_country'], axis = 0, sort = False)['value'].sum()
        save_df_to_hdf(df_wiod_aggregate_by_year[year], 'wiod_aggregate_data.hdf5', 'year_' + str(year))
    return df_wiod_aggregate_by_year


# get wiod data into hdf5
years = range(1995, 2011)
df_wiod_by_year = wiod_parser([2010])

# import wiod data
df_wiod_by_year = import_wiod_hdf([2010])

# compute aggregate wiod
df_wiod_aggregate_by_year = wiod_aggregate(df_wiod_by_year)


#
## Comparison EORA - WIOD
#
#df = eora_df(list_years, list_prices, 0)
#df_aggregate_stacked, df_aggregate = aggregate_eora(df)
#df_aggregate_to_csv(df_aggregate)
#
## COMPARISON
#result = pandas.concat([df_aggregate_stacked, df_wiod_2010_aggregate_long], axis = 1, join = 'inner')
#result[1] = result[1] * 1000
#result['diff'] = result[1] - result[0]
#result['diff_pct'] = 100 * result['diff'] / result[0]
