# -*- coding: utf-8 -*-
"""
Created on Fri Aug 07 09:54:33 2015

@author: Antoine
"""

# -*- coding: utf-8 -*-
"""
Created on Sat Jul 18 16:58:27 2015

@author: Antoine
"""
import pandas
import numpy

import logging
import os
import shutil
import zipfile
import urllib

import pkg_resources


from yale_eora.config import Config


log = logging.getLogger(__name__)

parser = Config(
    config_files_directory = os.path.join(pkg_resources.get_distribution('yale-eora').location)
    )

eora_directory = parser.get('data', 'eora_directory')
eora26_directory = parser.get('data', 'eora26_directory')
eora_aggregated_directory = parser.get('data', 'eora_aggregated_directory')
eora_output_directory = parser.get('data', 'eora_output_directory')


def getunzipped(theurl, thedir, thename):
    # download the file from theurl into thedir,
    # give it the name thename,
    # and unzip it in the same folder
    name = os.path.join(thedir, thename)
    # name is the path + name + extension of zip file
    if not os.path.exists(thedir):
        os.makedirs(thedir)
    try:
        name, hdrs = urllib.urlretrieve(theurl, name)
    except IOError, e:
        print "Can't retrieve %r to %r: %s" % (theurl, thedir, e)
        return
    try:
        z = zipfile.ZipFile(name)
    except zipfile.error, e:
        print "Bad zipfile (from %r): %s" % (theurl, e)
        return
    z.close()

    with zipfile.ZipFile(name) as zip_file:
        for member in zip_file.namelist():
            filename = os.path.basename(member)
            # skip directories
            if not filename:
                continue  # go on with next iteration of the loop

            # copy file (taken from zipfile's extract)
            source = zip_file.open(member)
            target = file(os.path.join(thedir, filename), "wb")
            with source, target:
                shutil.copyfileobj(source, target)


def eora_downloader(year, price, aggregated):
    if aggregated == 0:
        theurl = 'http://www.worldmrio.com//ComputationsE/Phase199/Loop082/simplified/Eora26_{}_{}.zip'.format(year, price)
        thedir = os.path.join(eora26_directory, 'Eora26_{}_{}'.format(year, price))
        thename = 'Eora26_{}_{}.zip'.format(year, price)
    else:
        theurl = 'http://worldmrio.com/dl.jsp?aishadirf=true&file=Summary/{}/Summary_{}.zip'.format(year, year)
        thedir = os.path.join(eora_aggregated_directory, 'Eora_aggregated_{}'.format(year))
        thename = 'Eora_aggregated_{}.zip'.format(year)
    getunzipped(theurl, thedir, thename)


def eora_downloader_list(years, prices, aggregated_list):
    for year in years:
        for aggregated in aggregated_list:
            if aggregated == 0:
                for price in prices:
                    eora_downloader(year, price, aggregated)
            if aggregated == 1 :
                eora_downloader(year, None, aggregated)


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
    df = df[dfs[0].columns]
    #  here, it classes columns alphabetically, such that columns from FD are mixed in column from T. to change?
    # look for: concatenate keeping order of columns on internet
    res = (df_T, df_VA, df_FD, df)
    # res = {'T': df_T, 'VA': df_VA, 'FD': df_FD, 'output': df}
    return res


def file_aggregated_parser():
    pass


def output_result(df, thedir, thename):
    df.to_csv(path_or_buf = os.path.join(thedir, thename), sep = '\t')  # , header = True, index = True)


def parse_and_store_eora_data(list_years, list_prices, aggregated = 0):
    df_by_year = dict()
    for year in list_years:
        for price in list_prices:
            if aggregated == 1:
                thedir = os.path.join(eora26_directory, "Eora26_{}_{}".format(year, price))
                thedir_output = os.path.join(eora_output_directory, 'aggregated', "Eora26_{}_{}".format(year, price))
            else:
                thedir = os.path.join(eora_aggregated_directory, "Eora_aggregated_{}".format(year))
                thedir_output = os.path.join(eora_output_directory, '25sector', "Eora26_{}_{}".format(year, price))

            T_file = os.path.join(thedir, "Eora26_{}_{}_T.txt".format(year, price))
            VA_file = os.path.join(thedir, "Eora26_{}_{}_VA.txt".format(year, price))
            FD_file = os.path.join(thedir, "Eora26_{}_{}_FD.txt".format(year, price))
            T_label = os.path.join(thedir, 'labels_T.txt')
            VA_label = os.path.join(thedir, 'labels_VA.txt')
            FD_label = os.path.join(thedir, 'labels_FD.txt')

            # df_by_keys = file_parser(T_file, VA_file, FD_file, T_label, VA_label, FD_label)
            # output_result(df_by_keys['output'], os.path.join(data_directory, 'output'), 'Eora26_{}_{}.txt'.format(year, price))
            if aggregated == 1:
                pass
            if aggregated == 0:
                (df_T, df_VA, df_FD, df) = file_parser(T_file, VA_file, FD_file, T_label, VA_label, FD_label)
                df_by_year[int(year)] = df
                output_result(df, thedir_output, 'Eora26_{}_{}.txt'.format(year, price))
            # df_by_year[int(year)] = [df_T, df_VA, df_FD, df]

    return df_by_year



list_years = [2010, 2011]  # range(2010, 2010)
list_prices = ['bp', 'pp']  # ['bp', 'pp']
aggregated_list = [1]  # 0 for 25sector, and 1 for aggregated data

eora_downloader_list(list_years, list_prices, aggregated_list)
parse_and_store_eora_data(years, list_prices, aggregated)


download_flag = 1  # 1 for download, 0 for parsing data only.
df_by_year = eora_df(list_years, ['bp'], 0, [0])
df_2010 = df_by_year[2010]
df_2011 = df_by_year[2011]
download_flag = None
