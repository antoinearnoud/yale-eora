# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 16:58:27 2015

@author: Antoine

"""
import argparse
import logging
import numpy
import os
import pandas
import pkg_resources
import sys

from yale_eora.config import Config


log = logging.getLogger(__name__)

parser = Config(
    config_files_directory = os.path.join(pkg_resources.get_distribution('yale-eora').location)
    )

eora_directory = parser.get('data', 'eora_directory')
eora26_directory = parser.get('data', 'eora26_directory')

eora_output_directory = parser.get('data', 'eora_output_directory')


def file_parser(T_file, VA_file, FD_file, T_label_file, VA_label_file, FD_label_file):
    df_T = pandas.read_csv(T_file, sep = '\t', header = None)
    df_VA = pandas.read_csv(VA_file, sep = '\t', header = None)
    df_FD = pandas.read_csv(FD_file, sep = '\t', header = None)

    df_T_label = pandas.read_csv(T_label_file, sep = '\t', header = None)
    df_VA_label = pandas.read_csv(VA_label_file, sep = '\t', header = None)
    df_FD_label = pandas.read_csv(FD_label_file, sep = '\t', header = None)

    # Construction of the 3 inner tables
    df = df_T
    df_T = df.set_index([df_T_label[1], df_T_label[3]], drop = True, verify_integrity = False)
    df_T.index.name = ['country_row', 'code_row']
    df_T = df_T.reindex(df_T.index.rename(['country_row', 'code_row']))
    df_T.columns = pandas.MultiIndex.from_arrays([numpy.array(df_T_label[1]),
                                                numpy.array(df_T_label[3])], names = ['country_col', 'code_col'])

    df = df_VA
    df_VA = df.set_index([df_VA_label[0], df_VA_label[1]], drop = True, verify_integrity = False)
    df_VA.index.name = ['country_row', 'code_row']
    df_VA = df_VA.reindex(df_VA.index.rename(['country_row', 'code_row']))
    df_VA.columns = pandas.MultiIndex.from_arrays([numpy.array(df_T_label[1]),
                                                numpy.array(df_T_label[3])], names = ['country_col', 'code_col'])

    df = df_FD
    df_FD = df.set_index([df_T_label[1], df_T_label[3]], drop = True, verify_integrity = False)
    df_FD.index.name = ['country_row', 'code_row']
    df_FD = df_FD.reindex(df_FD.index.rename(['country_row', 'code_row']))
    df_FD.columns = pandas.MultiIndex.from_arrays([numpy.array(df_FD_label[1]),
                                                numpy.array(df_FD_label[3])], names = ['country_col', 'code_col'])

    # label dataframes
    df = pandas.concat([df_T, df_FD], axis = 1)
    dfs = [df, df_VA]
    df = pandas.concat(dfs, axis = 0)
    df = df[dfs[0].columns]  # why 0 ?
    #  here, it classes columns alphabetically, such that columns from FD are mixed in column from T. to change?
    # look for: concatenate keeping order of columns on internet
    res = (df_T, df_VA, df_FD, df)
    # res = {'T': df_T, 'VA': df_VA, 'FD': df_FD, 'output': df}
    return res


def eora_parser(year, price):
            thedir = os.path.join(eora26_directory, 'Eora26_{}_{}'.format(year, price))
            T_file = os.path.join(thedir, "Eora26_{}_{}_T.txt".format(year, price))
            VA_file = os.path.join(thedir, "Eora26_{}_{}_VA.txt".format(year, price))
            FD_file = os.path.join(thedir, "Eora26_{}_{}_FD.txt".format(year, price))
            T_label_file = os.path.join(thedir, 'labels_T.txt')
            VA_label_file = os.path.join(thedir, 'labels_VA.txt')
            FD_label_file = os.path.join(thedir, 'labels_FD.txt')
            (df_T, df_VA, df_FD, df) = file_parser(T_file, VA_file, FD_file, T_label_file, VA_label_file, FD_label_file)
            return df_T, df_VA, df_FD, df


def df_to_csv(df, thedir, thename):
    df.to_csv(path_or_buf = os.path.join(thedir, thename), sep = '\t')  # , header = True, index = True)


def df_to_hdf(df, hdf_file_name, key):
    file_path = os.path.join(eora_output_directory, '26sector', hdf_file_name)
    df.to_hdf(file_path, key)
    pandas.DataFrame().to_hdf


def parse_and_save_many(years, prices):
    df_by_year_and_price = dict()
    T_by_year_and_price = dict()
    VA_by_year_and_price = dict()
    FD_by_year_and_price = dict()
    for year in years:
        for price in prices:
            (df_T, df_VA, df_FD, df) = eora_parser(year, price)
            df_by_year_and_price[(int(year), str(price))] = df
            T_by_year_and_price[int(year), str(price)] = df_T
            VA_by_year_and_price[int(year), str(price)] = df_VA
            FD_by_year_and_price[int(year), str(price)] = df_FD
            df_to_csv(df, os.path.join(eora_output_directory, '26sector'), 'Eora26_{}_{}.txt'.format(year, price))
            # df_to_csv(df_T, os.path.join(eora_output_directory, '26sector'), 'Eora26_{}_{}_T.txt'.format(year, price))
            # save_df_to_hdf(df, 'Eora26_{}_{}.hdf5'.format(year, price), 'df')
            # uncomment below
            df_to_hdf(df_T, 'Eora26_{}_{}.hdf5'.format(year, price), 'T')
            df_to_hdf(df_VA, 'Eora26_{}_{}.hdf5'.format(year, price), 'VA')
            df_to_hdf(df_FD, 'Eora26_{}_{}.hdf5'.format(year, price), 'FD')
    return df_by_year_and_price, T_by_year_and_price, VA_by_year_and_price, FD_by_year_and_price


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--end', default = 2012, help = 'ending year to be downloaded')
    parser.add_argument('-s', '--start', default = 1970, help = 'starting year to be downloaded')
    # parser.add_argument('-t', '--target', default = eora26_directory, help = 'path where to store downloaded files')
    parser.add_argument('-p', '--price', default = ['bp', 'pp'], help = 'prices to be downloaded')
    parser.add_argument('-v', '--verbose', action = 'store_true', default = False, help = "increase output verbosity")
    args = parser.parse_args()
    logging.basicConfig(level = logging.DEBUG if args.verbose else logging.WARNING, stream = sys.stdout)
    args.start = int(args.start)
    args.end = int(args.end)
    parse_and_save_many(years = range(args.start, args.end + 1), prices = args.price)

if __name__ == "__main__":
    sys.exit(main())


# list_years = range(1995, 1996)  # range(2010, 2010)
# list_prices = ['bp', 'pp']  # ['bp', 'pp']
# df_by_year_and_price, T_by_year_and_price, VA_by_year_and_price, FD_by_year_and_price = parse_and_save_many(list_years, list_prices)
