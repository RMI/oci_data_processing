import pandas as pd
import os
from os.path import join    

sp_dir= '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'

print('Extracting product slates and emission data from Liam batch run results...')
nondefault_directory_path = {'haverly': sp_dir +'/Deep Dive page/Midstream/nondefault runs new/haverly 20y',
                              'oci':sp_dir + '/Deep Dive page/Midstream/nondefault runs new/oci 20y',
                              'prelim':sp_dir + '/Deep Dive page/Midstream/nondefault runs new/prelim 20y'}

nondefault_assay_file_list = dict()
for assay_group in nondefault_directory_path:
    nondefault_assay_file_list[assay_group] = []
    for file in os.listdir(nondefault_directory_path[assay_group]):
        filename = os.fsdecode(file)
        if filename.endswith('.xlsx'): 
#            print(filename)
            nondefault_assay_file_list[assay_group].append(join(nondefault_directory_path[assay_group], filename))


def extract_product_slate(file):
    """A function that extracts product slate mass flows from the a batch run file.
    The input of is the file direcotyr and the output is a column of product slate"""
    df = pd.read_excel(file,sheet_name='Sheet1', header = None)
    assay_id = file.split('/')[-1].split('_')[0]
    assay_name = df.iloc[0,0]
    #print(assay_name)
    df = df.iloc[:,1]
    #get emission fraction data from the 3d tab (use Coking Refinery)  
    emission = pd.read_excel(file,sheet_name='Sheet3', header = None)

    df = pd.concat([df,emission.iloc[1,0:3].T],axis=0)
    default_refinery = df.iloc[0] #'_'.join(df.iloc[0].split('_')[:-2])
    df = df.reset_index()
    #print(df)
    df = df.drop(columns ='index')
    
    df.iloc[0,0] = assay_name

    df.iloc[1,0] = default_refinery
    df.iloc[2,0]=assay_id
    #assay_name
    return df

def extract_product_slate_7c(file):
    """A function that extracts product slate mass flows from the a batch run file.
    The input of is the file direcotyr and the output is a column of product slate"""
    df = pd.read_excel(file,sheet_name='Sheet1', header = None)
    assay_id = file.split('/')[-1].split('_')[0]
    assay_name = df.iloc[0,0]
    #print(assay_name)
    df = df.iloc[:,2]
    #get emission fraction data from the 3d tab (use Coking Refinery)  
    emission = pd.read_excel(file,sheet_name='Sheet3', header = None)

    df = pd.concat([df,emission.iloc[3,0:3].T],axis=0)
    default_refinery = df.iloc[0] #'_'.join(df.iloc[0].split('_')[:-2])
    df = df.reset_index()
    #print(df)
    df = df.drop(columns ='index')
    
    df.iloc[0,0] = assay_name

    df.iloc[1,0] = default_refinery
    df.iloc[2,0]=assay_id
    #assay_name
    return df


# The parameter names are extracted into a series 
parameter = pd.read_excel(nondefault_assay_file_list['oci'][0],sheet_name=0, header=None)
parameter = parameter.iloc[:,0]

parameter = pd.concat([parameter,pd.DataFrame(['emission_frac_CO2','emission_frac_CH4','emission_frac_N2O'])],axis =0)
parameter = parameter.reset_index().drop(columns ='index')
parameter.iloc[0,0]='parameter'
parameter.iloc[1,0]='Refinery Type'
parameter.iloc[2,0]='assay_id'

# store all nondefault product slates in a dictionary with assay group as keys and assay product slate dataframes as values
nondefault_assay_slates_df = {'haverly':dict(),'prelim':dict(),'oci':dict()}
for assay_group in nondefault_assay_file_list:
    #print(assay_group)
    nondefault_assay_slates_df[assay_group] = pd.DataFrame()
    for assay_file in nondefault_assay_file_list[assay_group]:        
        #print(assay_file)
        if assay_file.endswith('1.xlsx') or assay_file.endswith('4.xlsx'):
            nondefault_assay_slates_df[assay_group] = pd.concat([nondefault_assay_slates_df[assay_group],extract_product_slate(assay_file)],axis=1)
        elif assay_file.endswith('7.xlsx'):
            nondefault_assay_slates_df[assay_group]= pd.concat([nondefault_assay_slates_df[assay_group],extract_product_slate(assay_file)],axis = 1)
            nondefault_assay_slates_df[assay_group]= pd.concat([nondefault_assay_slates_df[assay_group],extract_product_slate_7c(assay_file)],axis = 1)


for assay_group in nondefault_assay_slates_df:
    #print(refinery_type)
    df =pd.concat([parameter,nondefault_assay_slates_df[assay_group]],axis=1)
    df.columns = df.iloc[0] 
    df = df[1:]
    df = df.T
    df.columns = df.iloc[0]
    df = df[1:]
    df = df.astype('float',errors='ignore')  
    nondefault_assay_slates_df[assay_group]=df


final_assay_library = pd.concat(nondefault_assay_slates_df,axis = 0)

final_assay_library.reset_index(inplace = True)

final_assay_library.rename(columns={'level_0':'assay_group',0:'assay_name'},inplace = True)

final_assay_library['assay_name'] = final_assay_library['assay_name'].apply(lambda x: x.strip())

# Use the twenty year file names to get correct assay names. Reason: 100 year file name is not clean. 
twentyyr_path = {'haverly': sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/Old Results/Haverly 20y',
                          'oci': sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/Old Results/OCI 20y',
                          'prelim': sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/Old Results/PRELIM 20y'}                              

def assay_name_20yr(assay_group,assay_id):
    '''return assay_name_20yr in the 20 year direcotry based on file names. 
    The assay_name_20yr will be used as one of the keys to match with sulphate and gravity data table'''
    
    for filename in os.listdir(twentyyr_path[assay_group]):
        if filename.split('_')[0]==assay_id:
            assay_name = '_'.join(filename.split('_')[1:])[:-5]
            return assay_name.strip()

final_assay_library['assay_name']=final_assay_library.apply(lambda x: assay_name_20yr(x['assay_group'],x['assay_id']),axis=1)

# Get throughput and sulfur content values from the three assay files and merge into the assay library



#final_assay_library

assay_files = {'haverly':[sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (100-y GWP)/Haverly PRELIM Assays.xlsx',535],
              'prelim':[sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (100-y GWP)/PRELIM Assays.xlsx',149],
              'oci':[sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (100-y GWP)/OCI Assay List Expanded 107.xlsx',107]}

assay_bbl_sulfur=dict()
for assay in assay_files:
    assay_df=pd.read_excel(assay_files[assay][0],header=None)
    assay_name = []
    assay_throughput =[]
    assay_sulfur = []
    df = pd.DataFrame()
    for i in range(assay_files[assay][1]):
        assay_name.append(assay_df.iloc[i*15,0].strip())
        assay_throughput.append(float(assay_df.iloc[3+i*15,2]))
        assay_sulfur.append(float(assay_df.iloc[6+i*15,2]))  
    df['assay_name']=assay_name
    df['throughput']=assay_throughput
    df['sulfur']=assay_sulfur
    assay_bbl_sulfur[assay]=df
    

assay_bbl_sulfur_library = pd.concat(assay_bbl_sulfur,axis = 0)

assay_bbl_sulfur_library.reset_index(level = 0, inplace = True)

assay_bbl_sulfur_library.rename(columns ={'level_0':'assay_group'},inplace = True)

assay_bbl_sulfur_library.reset_index(inplace = True)

assay_bbl_sulfur_library.drop(columns = 'index',inplace = True)

#Take the first assay of duplicate assays in each assay group
assay_bbl_sulfur_library = assay_bbl_sulfur_library.groupby(['assay_group','assay_name']).first()

assay_bbl_sulfur_library.reset_index(inplace = True)

final_assay_library_merged = final_assay_library.merge(assay_bbl_sulfur_library,how = 'left',indicator = True)

if final_assay_library_merged[final_assay_library_merged['_merge']!='both'].shape[0]>0:
    print('unmerged, check results.')
else:
    final_assay_library_merged.drop(columns = '_merge')

final_assay_library_merged.to_excel(sp_dir + '/Deep Dive page/Analytics/nondefault_assay_library.xlsx',index = False)


import sqlite3
connection = sqlite3.connect(sp_dir+"/OCI_Database.db")
scenario_fields = pd.read_sql('select DISTINCT Field_name from scenario_upstream_results', connection)


field_assay = pd.read_sql('select opgee_field, opgee_country, assay_category, assay_id from midstream_results where opgee_field in (select DISTINCT Field_name from scenario_upstream_results)',connection)
field_assay['assay_id'] = field_assay['assay_id'].apply(lambda x: str(int(x)))



merged_df = field_assay.merge(final_assay_library_merged,left_on=['assay_id','assay_category'],right_on=['assay_id','assay_group'],how = 'left')


merged_df.to_excel(sp_dir +'/Deep Dive page/Analytics/nondefault_field_assay_slate_emission.xlsx',index = False)

# Reload the file from Excel to automatically naming 
# the columns with numbers to avoid duplicated column names
merged_df = pd.read_excel(sp_dir + '/Deep Dive page/Analytics/nondefault_field_assay_slate_emission.xlsx')

# numerical_columns = [
#  'Gasoline',
#  'Jet Fuel',
#  'Diesel',
#  'Fuel Oil',
#  'Petroleum Coke',
#  'Residual Fuels',
#  'Refinery Fuel Gas (RFG)',
#  'Blended Gasoline',
#  'Jet-A/AVTUR',
#  'ULSD',
#  'Fuel Oil.1',
#  'Coke',
#  'Liquid Heavy Ends',
#  'RFG',
#  'Surplus NCR H2',
#  'Liquified Petroleum Gas (LPG)',
#  'Petrochemical Feedstocks',
#  'Asphalt',
#  'Gasoline S wt%',
#  'Gasoline H2 wt%',
#  'Blended Gasoline.1',
#  'Jet-A/AVTUR.1',
#  'ULSD.1',
#  'Fuel Oil.2',
#  'Coke.1',
#  'Liquid Heavy Ends.1',
#  'Sulphur',
#  'RFG.1',
#  'Surplus NCR H2.1',
#  'Liquified Petroleum Gas (LPG).1',
#  'Petrochemical Feedstocks.1',
#  'Asphalt.1',
#  'Crude',
#  'Electricity',
#  'Heat',
#  'RFG.2',
#  'Natural Gas',
#  'Steam',
#  'RFG.3',
#  'Natural Gas.1',
#  'Electricity.1',
#  'Hydrogen via SMR',
#  'Steam RFG',
#  'Steam Natural Gas',
#  'Steam Electricity',
#  'RFG, heat SMR',
#  'Natural Gas, Heat SMR',
#  'Natural Gas, Feed SMR',
#  'SMR Process Emissions',
#  'Hydrogen via CNR',
#  'Other Emissions',
#  'Subprocess Emissions',
#  'Support Services Emissions',
#  'Releases from Managed Wastes',
#  'Total refinery processes',
#  'Blended Gasoline.2',
#  'Jet-A/AVTUR.2',
#  'ULSD.2',
#  'Fuel Oil.3',
#  'Coke.2',
#  'Liquid Heavy Ends.2',
#  'Sulfur (emissions per kg of sulfur)',
#  'RFG.4',
#  'Surplus NCR H2.2',
#  'Liquified Petroleum Gas (LPG).2',
#  'Petrochemical Feedstocks.2',
#  'Asphalt.2',
#  'Crude.1',
#  'Electricity.2',
#  'Heat.1',
#  'RFG.5',
#  'Natural Gas.2',
#  'Steam.1',
#  'RFG.6',
#  'Natural Gas.3',
#  'Electricity.3',
#  'Hydrogen via SMR.1',
#  'Steam RFG.1',
#  'Steam Natural Gas.1',
#  'Steam Electricity.1',
#  'RFG, heat SMR.1',
#  'Natural Gas, Heat SMR.1',
#  'Natural Gas, Feed SMR.1',
#  'SMR Process Emissions.1',
#  'Hydrogen via CNR.1',
#  'Other Emissions.1',
#  'Subprocess Emissions.1',
#  'Support Services Emissions.1',
#  'Releases from Managed Wastes.1',
#  'Total refinery processes.1',
#  'emission_frac_CO2',
#  'emission_frac_CH4',
#  'emission_frac_N2O',
# 'throughput',
# 'sulfur']

# # calculate product slates and emissions for composite assays
# merged_df[numerical_columns] = merged_df[numerical_columns].multiply(merged_df['normalized_ratio'], axis="index")



# non_numerical = [i for i in merged_df.columns.to_list() if i not in numerical_columns]

# non_numerical.remove('ratio')

# non_numerical.remove('normalized_ratio')

# field_slate_emission = merged_df.groupby(['opgee_field','opgee_country'])[numerical_columns].agg('sum')


# field_slate_emission[['Confidence','assay_group','assay_id','correct_assay_name','Default Refinery',
#                       'normalized_ratio']] = merged_df.groupby(['opgee_field','opgee_country'])['Confidence','assay_group','assay_id','correct_assay_name','Default Refinery','normalized_ratio'].agg('first')


# field_slate_emission = field_slate_emission.reset_index()

import numpy as np
field_slate_emission = merged_df
#field_slate_emission['matched_assay']=field_slate_emission['assay_name']
#field_slate_emission.drop(columns ='correct_assay_name',inplace = True)
field_slate_emission['GWP']='20yr'

field_slate_emission.to_excel(sp_dir + '/Deep Dive page/Analytics/nondefault_field_slate_emission.xlsx',index = False)

print('Updating midstream results in OCI database...')

field_slate_emission.to_sql('scenario_midstream_results_nondefault',connection, if_exists='replace', index=False)

print('Midstream updates completed.')
