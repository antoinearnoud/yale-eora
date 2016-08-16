# -*- coding: utf-8 -*-
"""
Created on Tue Sep 15 11:38:53 2015

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


# IMPORT EORA 26 DATA
def import_hdf_to_df(hdf_file_path, key):
    store = pandas.HDFStore(hdf_file_path)
    df = store[key]
    return df


def import_eora26_from_hdf_to_df_dict(years, prices):
    # df_by_year_and_price = dict()
    T_by_year_and_price = dict()
    VA_by_year_and_price = dict()
    FD_by_year_and_price = dict()
    for year in years:
        for price in prices:
            hdf_file_path = os.path.join(eora_output_directory,
            '26sector', 'Eora26_{}_{}.hdf5'.format(year, price))
            print hdf_file_path
            # df_by_year_and_price[(year, str(price))] = import_hdf_to_df(hdf_file_path, 'df')
            T_by_year_and_price[(year, str(price))] = import_hdf_to_df(hdf_file_path, 'T')
            VA_by_year_and_price[(year, str(price))] = import_hdf_to_df(hdf_file_path, 'VA')
            FD_by_year_and_price[(year, str(price))] = import_hdf_to_df(hdf_file_path, 'FD')
    return T_by_year_and_price, VA_by_year_and_price, FD_by_year_and_price


def import_eora26_from_hdf_to_dict_single_data(year, price):
    hdf_file_path = os.path.join(eora_output_directory, '26sector', 'Eora26_{}_{}.hdf5'.format(year, price))
    print hdf_file_path
    # df_by_year_and_price[(year, str(price))] = import_hdf_to_df(hdf_file_path, 'df')
    T_df = import_hdf_to_df(hdf_file_path, 'T')
    VA_df = import_hdf_to_df(hdf_file_path, 'VA')
    FD_df = import_hdf_to_df(hdf_file_path, 'FD')
    return T_df, VA_df, FD_df

T_df, VA_df, FD_df = import_eora26_from_hdf_to_dict_single_data(2010, 'bp')
T_by_year_and_price, VA_by_year_and_price, FD_by_year_and_price = import_eora26_from_hdf_to_df_dict([2010, 2011], ['bp', 'pp'])
