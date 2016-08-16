# -*- coding: utf-8 -*-
"""
Created on Sun Aug 30 17:17:26 2015

@author: Antoine
"""
import logging
import os
import pandas
import pickle
import pkg_resources


from yale_eora.config import Config


log = logging.getLogger(__name__)

parser = Config(
    config_files_directory = os.path.join(pkg_resources.get_distribution('yale-eora').location)
    )

eora_directory = parser.get('data', 'eora_directory')
eora26_directory = parser.get('data', 'eora26_directory')
eora_summary_directory = parser.get('data', 'eora_summary_directory')

eora_output_directory = parser.get('data', 'eora_output_directory')


def file_parser(file_name):
    df_summary = pandas.read_csv(file_name, header = 0, index_col = 0)
    return df_summary


# FOR NOW, ONLY PARSE M1, i.e. BASIC PRICE I THINK
def eora_summary_parser(year):
            thedir = os.path.join(eora_summary_directory, 'Eora_summary_{}'.format(year))
            file_name = os.path.join(thedir, "Summary_{}_m1.csv".format(year))
            df = file_parser(file_name)
            return df


def import_hdf_to_df(hdf_file_name, thedir, key):
    file_path = os.path.join(thedir, hdf_file_name)
    store = pandas.HDFStore(file_path)
    df = store[key]
    return df

# COMPARISON BETWEEN EORA_SUMMARY DATA AND EORA_26 DATA aggregated
df_summary_2010 = eora_summary_parser(2010)
df_eora_2010_agg = import_hdf_to_df('Eora26_2010_bp_aggregated.hdf5', os.path.join(eora_output_directory,'26sector_aggregated'), 'df_csv')

df_diff = df_eora_2010_agg.subtract(df_summary_2010)
df_diff_pct = df_diff.div(df_eora_2010_agg)
df_diff_pct = 100.0 * df_diff_pct
with open(os.path.join(eora_output_directory, 'cols_full_name_ordered.bin'), "rb") as data:
    cols_full_name_ordered = pickle.load(data)
with open(os.path.join(eora_output_directory, 'index_full_name_ordered.bin'), "rb") as data:
    index_full_name_ordered = pickle.load(data)

df_diff = df_diff[cols_full_name_ordered]
df_diff = df_diff.reindex(index_full_name_ordered)

df_diff_pct = df_diff_pct[cols_full_name_ordered]
df_diff_pct = df_diff_pct.reindex(index_full_name_ordered)
df_diff_pct.to_csv(os.path.join(eora_output_directory, '26sector_aggregated', 'Eora26_{}_{}_aggregated_summary_diff_pct.txt'.format(2010, 'bp')), sep = '\t')

