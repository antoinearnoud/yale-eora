# -*- coding: utf-8 -*-
"""
Created on Tue Aug 11 10:08:13 2015

@author: Antoine
"""


import os
import pandas
import pkg_resources


from yale_eora.config import Config

parser = Config(
    config_files_directory = os.path.join(pkg_resources.get_distribution('yale-eora').location)
    )

eora_output_directory = parser.get('data', 'eora_output_directory')
wiod_output_directory = parser.get('data', 'wiod_output_directory')
wiod_data = parser.get('data', 'wiod_data')

def import_hdf_to_df(hdf_file_name, thedir, key):
    file_path = os.path.join(thedir, hdf_file_name)
    store = pandas.HDFStore(file_path)
    df = store[key]
    return df


# df_eora_2010_agg = import_hdf_to_df('Eora26_2010_bp_aggregated.hdf5', os.path.join(eora_output_directory,'26sector_aggregated'), 'df_csv')

#CHOOSE ONE:
#df_eora_1995_agg = import_hdf_to_df('Eora26_1995_bp_aggregated.hdf5', os.path.join(eora_output_directory,'26sector_aggregated'), 'df')
df_eora_1995_agg = import_hdf_to_df('Eora26_1995_pp_aggregated.hdf5', os.path.join(eora_output_directory,'26sector_aggregated'), 'df')
# df_eora_1995_agg_csv = import_hdf_to_df('Eora26_1995_bp_aggregated.hdf5', os.path.join(eora_output_directory,'26sector_aggregated'), 'df_csv')
# df_eora_2010_agg_T = import_hdf_to_df('Eora26_2010_bp_aggregated.hdf5', os.path.join(eora_output_directory,'26sector_aggregated'), 'T')
# df_eora_2010_agg_VA = import_hdf_to_df('Eora26_2010_bp_aggregated.hdf5', os.path.join(eora_output_directory,'26sector_aggregated'), 'VA')
# df_eora_2010_agg_FD = import_hdf_to_df('Eora26_2010_bp_aggregated.hdf5', os.path.join(eora_output_directory,'26sector_aggregated'), 'FD')

df_eora_1995_agg = df_eora_1995_agg.to_frame()
df_eora_1995_agg.columns = ['value']
df_eora_1995_agg = df_eora_1995_agg.unstack()
df_eora_1995_agg.columns = df_eora_1995_agg.columns.get_level_values(1)

# loads wiod dataset edited with "Primary Inputs" and suffix "_FD" when necessary
wiod_aggregate_df = pandas.read_stata(os.path.join(wiod_data, 'wiot_edited_1995.dta'))
wiod_aggregate_df = wiod_aggregate_df.set_index(['row_country', 'col_country'])
wiod_aggregate_df = wiod_aggregate_df.unstack()
wiod_aggregate_df.columns = wiod_aggregate_df.columns.get_level_values(1)
wiod_aggregate_df = wiod_aggregate_df * 1000000  # WIOD is in MILLION OF DOLLARS, IN CURRENT PRICE.

# keeps only IO table from boths
# wiod_aggregate_df = wiod_aggregate_df.drop(['Primary Inputs'], axis = 0)
# wiod_aggregate_df = wiod_aggregate_df.filter(regex='_FD')  # how to drop instead of keeping?


colnames_wiod = wiod_aggregate_df.columns
colnames_eora = df_eora_1995_agg.columns
colnames = colnames_eora.intersection(colnames_wiod)
colnames_list = colnames.tolist()
df_eora_1995_agg = df_eora_1995_agg[colnames_list]
wiod_aggregate_df = wiod_aggregate_df[colnames_list]
df_eora_1995_agg = df_eora_1995_agg.ix[colnames_list]
wiod_aggregate_df = wiod_aggregate_df.ix[colnames_list]

# compute the deviation in percentages
df_eora_1995_agg = df_eora_1995_agg*1000
diff_pct_1995 = df_eora_1995_agg.subtract(wiod_aggregate_df)
diff_pct_1995 = diff_pct_1995.div(df_eora_1995_agg)
diff_pct_1995 = 100 * diff_pct_1995


#diff_pct_1995.to_csv(os.path.join(eora_output_directory, 'woid_vs_eora_{}_{}_pct.txt'.format(1995, 'bp')), sep = '\t')
diff_pct_1995.to_csv(os.path.join(eora_output_directory, 'woid_vs_eora_{}_{}_pct.txt'.format(1995, 'pp')), sep = '\t')

#df_eora_1995_agg.to_csv(os.path.join(eora_output_directory, 'eora_aggregated_{}_{}.txt'.format(1995, 'bp')), sep = '\t')
df_eora_1995_agg.to_csv(os.path.join(eora_output_directory, 'eora_aggregated_{}_{}.txt'.format(1995, 'pp')), sep = '\t')

wiod_aggregate_df.to_csv(os.path.join(eora_output_directory, 'wiod_aggregated_2005.txt'.format(1995, 'pp')), sep = '\t')









wiod_aggregate_df = import_hdf_to_df('wiod_aggregate_data.hdf5', wiod_output_directory, 'year_' + str(2010)) # not working why? chemin d'acces wrong?
wiod_aggregate_df  = df_wiod_aggregate_by_year[2010]
wiod_aggregate_df = wiod_aggregate_df * 1000000


result = pandas.concat([df1,wiod_aggregate_df], axis = 1, join = 'inner')
result['Delta'] = (result[0] - result[1]) / result[1]
result[1] = result[1] * 1000
result['diff'] = result[1] - result[0]
result['diff_pct'] = 100 * result['diff'] / result[0]