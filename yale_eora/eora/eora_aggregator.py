# -*- coding: utf-8 -*-
"""
Created on Mon Jul 20 09:57:04 2015

@author: Antoine
"""


import os
import pandas
import pickle
import pkg_resources


from yale_eora.config import Config

parser = Config(
    config_files_directory = os.path.join(pkg_resources.get_distribution('yale-eora').location)
    )
eora_output_directory = parser.get('data', 'eora_output_directory')
eora26_directory = parser.get('data', 'eora26_directory')


# Import the files
def import_hdf_to_df(hdf_file_name, key):
    file_path = os.path.join(eora_output_directory, hdf_file_name)
    store = pandas.HDFStore(file_path)
    df = store[key]
    return df


def import_data(years, prices):
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


# aggregates data
def aggregate_eora(df):
    grouped = df.groupby(level=[0], axis = 1, sort = False).sum()
    df_aggregate = grouped.groupby(level=[0], axis = 0, sort = False).sum()
    df_aggregate_stacked = df_aggregate.stack()
    return df_aggregate_stacked  # , df_aggregate


def df_aggregate_to_csv(df_aggregate, year, price, table = None):
    if table is None:
        df_aggregate.to_csv(os.path.join(eora_output_directory, '26sector_aggregated', 'Eora26_{}_{}_aggregated.txt'.format(year, price)), sep = '\t')
        # , header = True, index = True)
    else:
        df_aggregate.to_csv(os.path.join(eora_output_directory, '26sector_aggregated', 'Eora26_{}_{}_aggregated_'.format(year, price) + table + '.txt'), sep = '\t')


def save_df_aggregated_to_hdf(df, hdf_file_name, key):
    file_path = os.path.join(eora_output_directory, '26sector_aggregated', hdf_file_name)
    df.to_hdf(file_path, key)
    pandas.DataFrame().to_hdf


def import_aggregate_save_many(years, prices):
    # import data
    T_by_year_and_price = dict()
    VA_by_year_and_price = dict()
    FD_by_year_and_price = dict()
    T_aggregated_by_year_and_price = dict()
    VA_aggregated_by_year_and_price = dict()
    FD_aggregated_by_year_and_price = dict()
    df_aggregated_by_year_and_price = dict()  # not really used
    T_by_year_and_price, VA_by_year_and_price, FD_by_year_and_price = import_data(years, prices)
    for year in years:
        for price in prices:
            T_aggregated = aggregate_eora(T_by_year_and_price[year, price])
            df_aggregate_to_csv(T_aggregated.unstack(1), year, price, 'T')
            save_df_aggregated_to_hdf(T_aggregated, 'Eora26_{}_{}_aggregated.hdf5'.format(year, price),'T')
            T_aggregated_by_year_and_price[year, price] = T_aggregated

            VA_aggregated = aggregate_eora(VA_by_year_and_price[year, price])
            df_aggregate_to_csv(VA_aggregated.unstack(1), year, price, 'VA')
            save_df_aggregated_to_hdf(VA_aggregated, 'Eora26_{}_{}_aggregated.hdf5'.format(year, price),'VA')
            VA_aggregated_by_year_and_price[year, price] = VA_aggregated

            FD_aggregated = aggregate_eora(FD_by_year_and_price[year, price])
            df_aggregate_to_csv(FD_aggregated.unstack(1), year, price, 'FD')
            save_df_aggregated_to_hdf(FD_aggregated, 'Eora26_{}_{}_aggregated.hdf5'.format(year, price),'FD')
            FD_aggregated_by_year_and_price[year, price] = FD_aggregated

            # df (dataframe)
            FD_temp = FD_aggregated.unstack(1)
            col_names = FD_aggregated.unstack(1).columns + '_FD'
            FD_temp.columns = col_names
            FD_temp = FD_temp.stack()
            df_aggregated = pandas.concat([T_aggregated, VA_aggregated, FD_temp])
            df_aggregated_by_year_and_price[year, price] = df_aggregated

            # to save csv file:
            df = df_aggregated.unstack(1)
            col_oredered = T_aggregated.unstack(1).columns.tolist() + FD_temp.unstack(1).columns.tolist()
            df= df[col_oredered]

            target_row = df.ix[['Primary Inputs'],:]
            df = df.drop(['Primary Inputs'])
            df = pandas.concat([df, target_row])

            thedir = os.path.join(eora26_directory, 'Eora26_{}_{}'.format(year, price))
            T_label_file = os.path.join(thedir, 'labels_T.txt')
            df_T_label = pandas.read_csv(T_label_file, sep = '\t', header = None)
            df_col_names = df_T_label[[0,1]]
            df_col_names = df_col_names.drop_duplicates()
            df_col_names =  df_col_names.set_index(1)
            dico_col_names = df_col_names.to_dict()
            dico_col_names = dico_col_names[0]
            dico_col_names2 = dict()
            for key in dico_col_names:
                dico_col_names2[key+'_FD'] = dico_col_names[key] +' - Final demand'
            df = df.rename(columns = dico_col_names)
            df = df.rename(columns = dico_col_names2)
            df = df.rename(index = dico_col_names)
            df = df.rename(index = {'Primary Inputs': 'Value added'})
            cols_full_name_ordered = df.columns.tolist()
            index_full_name_ordered = df.index.tolist()
            with open(os.path.join(eora_output_directory, 'cols_full_name_ordered.bin'), "wb") as output:
                pickle.dump(cols_full_name_ordered, output)
            with open(os.path.join(eora_output_directory, 'index_full_name_ordered.bin'), "wb") as output:
                pickle.dump(index_full_name_ordered, output)

            # df.sort_index(axis=1) instead of line below:
            # df.reindex_axis(sorted(df.columns), axis=1)
            df_aggregate_to_csv(df, year, price)
            save_df_aggregated_to_hdf(df, 'Eora26_{}_{}_aggregated.hdf5'.format(year, price),'df_csv') # csv version, comparable with summary data
            # to save hdf file:
            save_df_aggregated_to_hdf(df_aggregated, 'Eora26_{}_{}_aggregated.hdf5'.format(year, price),'df')

    return T_aggregated_by_year_and_price, VA_aggregated_by_year_and_price, FD_aggregated_by_year_and_price, df_aggregated_by_year_and_price

# Running on data
T_aggregated, VA_aggregated, FD_aggregated, df_aggregated = import_aggregate_save_many([1995], ['bp'])
T_aggregated, VA_aggregated, FD_aggregated, df_aggregated = import_aggregate_save_many([1995], ['pp'])
