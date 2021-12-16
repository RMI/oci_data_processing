import pandas as pd
import os
from os.path import join    

sp_dir= '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'

print('Extracting product slates and emission data from Liam batch run results...')
twentyyr_directory_path = [sp_dir +'/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/Haverly 20y',
                              sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/OCI 20y',
                              sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/PRELIM 20y']

twentyyr_assay_file_list = dict()
for directory in twentyyr_directory_path:
    folder = directory.split('/')[-1].split(' ')[0].lower()
    twentyyr_assay_file_list[folder] = []
    
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        if filename.endswith('.xlsx'): 
#            print(filename)
            twentyyr_assay_file_list[folder].append(join(directory, filename))

def extract_product_slate(file):
    """A function that extracts product slate mass flows from the a batch run file.
    The input of is the file direcotyr and the output is a column of product slate"""
    df = pd.read_excel(file,sheet_name=0, header = None)
    assay_name = df.iloc[0,0]
    #print(assay_name)
    df = df.iloc[:,1]
    #get emission fraction data from the 3d tab (use Coking Refinery)  
    emission = pd.read_excel(file,sheet_name=2, header = None)
    
    #print(df)
    df = pd.concat([df,emission.iloc[1,0:3].T],axis=0)
    default_refinery = '_'.join(df.iloc[0].split('_')[:-2])
    df = df.reset_index()
    #print(df)
    df = df.drop(columns ='index')
    
    df.iloc[0,0] = assay_name
    df.iloc[1,0] = default_refinery
    #assay_name
    return df


# The parameter names are extracted into a series 
parameter = pd.read_excel(twentyyr_assay_file_list['oci'][0],sheet_name=0, header=None)
parameter = parameter.iloc[:,0]

parameter = pd.concat([parameter,pd.DataFrame(['emission_frac_CO2','emission_frac_CH4','emission_frac_N2O'])],axis =0)
parameter = parameter.reset_index().drop(columns ='index')

parameter.iloc[0,0]='parameter'
parameter.iloc[1,0]='Default Refinery'

# store all twenty year assay product slates in a dictionary with assay group as keys and assay product slate dataframes as values
twentyyr_assay_slates_df = dict()
for assay_group in twentyyr_assay_file_list:
    #print(assay_group)
    df_list = []
    for assay_file in twentyyr_assay_file_list[assay_group]:
        df_list.append(extract_product_slate(assay_file))
        
        df =pd.concat([parameter,pd.concat(df_list,axis = 1)],axis=1)
        df.columns = df.iloc[0] 
        df = df[1:]
        df = df.T
        df.columns = df.iloc[0]
        df = df[1:]
        df = df.astype('float',errors='ignore')
        #df['assay_group']=assay_group
        twentyyr_assay_slates_df[assay_group]= df    

final_assay_library = pd.concat(twentyyr_assay_slates_df,axis = 0)

final_assay_library.reset_index(inplace = True)


final_assay_library.rename(columns={'level_0':'assay_group',0:'assay_name'},inplace = True)

#Take the first assay of duplicate assays in each assay group

final_assay_library = final_assay_library.groupby(['assay_group','assay_name']).first()

final_assay_library = final_assay_library.dropna(axis = 1,how ='all')

final_assay_library.reset_index(inplace = True)

final_assay_library.drop_duplicates(subset='assay_name',keep='first')

final_assay_library['assay_name'] = final_assay_library['assay_name'].apply(lambda x: x.strip())

final_assay_library.head()

# #### Skip skip skip, fuzzy match to match opgee assays to assay library
# #### The data has been stored in OCI_Database.db
# from fuzzywuzzy import fuzz
# def best_match(x,slates):
# #This function find the best matching field_country combination in soloman list
#     match_score = []
#     for i in range(len(slates)):
#         score = fuzz.token_set_ratio(x, slates[i])
#         match_score.append(score)
#     return slates[match_score.index(max(match_score))]

# df_assay_slate_match = pd.DataFrame()
# df_assay_slate_match['opgee_assay_list']=list(field_assay['Assay'].unique())

# df_assay_slate_match

# df_assay_slate_match['product_slate_match']=df_assay_slate_match['opgee_assay_list'].apply(lambda x: best_match(x,slates))

# pd.set_option("display.max_rows", 500)

# df_assay_slate_match.to_csv('opgee_assay_slate_match.csv')

# Get throughput and sulfur content values from the three assay files and merge into the assay library

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

final_assay_library_merged.to_excel(sp_dir + '/Midstream/Liam_Batchrun/Analytics/final_assay_library.xlsx',index = False)

import sqlite3
connection = sqlite3.connect(sp_dir+"/OCI_Database.db")


field_assay = pd.read_sql('select * from field_assay_mapping',connection)

#pd.set_option("display.max_rows", 999)

#field_assay['normalized_ratio'] = 1/field_assay.groupby(['opgee_field','opgee_country']).transform('count')['Assay']

#field_assay.drop(columns = 'index',inplace = True)

#field_assay.to_sql('field_assay_mapping',connection,if_exists = 'replace',index = False)

merged_df = field_assay.merge(final_assay_library_merged,left_on=['correct_assay_name','assay_category'],right_on=['assay_name','assay_group'],how = 'left')

merged_df.to_excel(sp_dir +'/Midstream/Liam_Batchrun/Analytics/field_assay_slate_emission.xlsx',index = False)



# Reload the file from Excel to automatically naming 
# the columns with numbers to avoid duplicated column names
merged_df = pd.read_excel(sp_dir + '/Midstream/Liam_Batchrun/Analytics/field_assay_slate_emission.xlsx')

numerical_columns = [
 'Gasoline',
 'Jet Fuel',
 'Diesel',
 'Fuel Oil',
 'Petroleum Coke',
 'Residual Fuels',
 'Refinery Fuel Gas (RFG)',
 'Blended Gasoline',
 'Jet-A/AVTUR',
 'ULSD',
 'Fuel Oil.1',
 'Coke',
 'Liquid Heavy Ends',
 'RFG',
 'Surplus NCR H2',
 'Liquified Petroleum Gas (LPG)',
 'Petrochemical Feedstocks',
 'Asphalt',
 'Gasoline S wt%',
 'Gasoline H2 wt%',
 'Blended Gasoline.1',
 'Jet-A/AVTUR.1',
 'ULSD.1',
 'Fuel Oil.2',
 'Coke.1',
 'Liquid Heavy Ends.1',
 'Sulphur',
 'RFG.1',
 'Surplus NCR H2.1',
 'Liquified Petroleum Gas (LPG).1',
 'Petrochemical Feedstocks.1',
 'Asphalt.1',
 'Crude',
 'Electricity',
 'Heat',
 'RFG.2',
 'Natural Gas',
 'Steam',
 'RFG.3',
 'Natural Gas.1',
 'Electricity.1',
 'Hydrogen via SMR',
 'Steam RFG',
 'Steam Natural Gas',
 'Steam Electricity',
 'RFG, heat SMR',
 'Natural Gas, Heat SMR',
 'Natural Gas, Feed SMR',
 'SMR Process Emissions',
 'Hydrogen via CNR',
 'Other Emissions',
 'Subprocess Emissions',
 'Support Services Emissions',
 'Releases from Managed Wastes',
 'Total refinery processes',
 'Blended Gasoline.2',
 'Jet-A/AVTUR.2',
 'ULSD.2',
 'Fuel Oil.3',
 'Coke.2',
 'Liquid Heavy Ends.2',
 'Sulfur (emissions per kg of sulfur)',
 'RFG.4',
 'Surplus NCR H2.2',
 'Liquified Petroleum Gas (LPG).2',
 'Petrochemical Feedstocks.2',
 'Asphalt.2',
 'Crude.1',
 'Electricity.2',
 'Heat.1',
 'RFG.5',
 'Natural Gas.2',
 'Steam.1',
 'RFG.6',
 'Natural Gas.3',
 'Electricity.3',
 'Hydrogen via SMR.1',
 'Steam RFG.1',
 'Steam Natural Gas.1',
 'Steam Electricity.1',
 'RFG, heat SMR.1',
 'Natural Gas, Heat SMR.1',
 'Natural Gas, Feed SMR.1',
 'SMR Process Emissions.1',
 'Hydrogen via CNR.1',
 'Other Emissions.1',
 'Subprocess Emissions.1',
 'Support Services Emissions.1',
 'Releases from Managed Wastes.1',
 'Total refinery processes.1',
 'emission_frac_CO2',
 'emission_frac_CH4',
 'emission_frac_N2O',
'throughput',
'sulfur']

# calculate product slates and emissions for composite assays
merged_df[numerical_columns] = merged_df[numerical_columns].multiply(merged_df['normalized_ratio'], axis="index")



non_numerical = [i for i in merged_df.columns.to_list() if i not in numerical_columns]

non_numerical.remove('ratio')

non_numerical.remove('normalized_ratio')

field_slate_emission = merged_df.groupby(['opgee_field','opgee_country'])[numerical_columns].agg('sum')


field_slate_emission[['Confidence','assay_category','assay_id','correct_assay_name','Default Refinery',
                      'normalized_ratio']] = merged_df.groupby(['opgee_field','opgee_country'])['Confidence','assay_category','assay_id','correct_assay_name','Default Refinery','normalized_ratio'].agg('first')


field_slate_emission = field_slate_emission.reset_index()

import numpy as np
field_slate_emission['matched_assay']=np.where(field_slate_emission['normalized_ratio']==1.0,field_slate_emission['correct_assay_name'],'Composite Proxy Assays')

field_slate_emission.drop(columns ='correct_assay_name',inplace = True)

field_slate_emission.to_excel(sp_dir + '/Midstream/Liam_Batchrun/Analytics/field_slate_emission.xlsx',index = False)

print('Updating midstream results in OCI database...')

field_slate_emission.to_sql('midstream_results',connection, if_exists='replace', index=False)

print('Midstream updates completed.')
