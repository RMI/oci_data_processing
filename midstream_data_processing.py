## Extract PRELIM results for OCI, PRELIM and Haverly assays, including product slates, emission, API gravity, and sulfur content

import pandas as pd
import os
from os.path import join    
sp_dir = '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'


print('Extracting product slates and emission data from Liam batch run results...')
onehundredyr_path= [sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (100-y GWP)/Haverly 100y',
                    sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (100-y GWP)/OCI 100y',
                    sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (100-y GWP)/PRELIM 100y']


twentyyr_path = [sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/New Results/Haverly 20y',
                sp_dir +  '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/New Results/OCI 20y', 
                sp_dir +  '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/New Results/PRELIM 20y']


# Remove files that are wrongly in the 100yr Haverly folder, based on the file name in new 20 yr haverly folder
# This is a one time fix and should only be run once 
# for filename in os.listdir(onehundredyr_path[0]):
#     if filename not in os.listdir(sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/New Results/Haverly 20y'):
#         print(filename)
#         os.remove(onehundredyr_path[0]+'/'+filename)

def extract_product_slate(file):
    """A function that extracts product slate mass flows from the a batch run file.
    The input of is the file direcotyr and the output is a column of product slate"""
    df = pd.read_excel(file,sheet_name=0, header = None)
    assay_id = file.split('/')[-1].split('_')[0]
    assay_name = df.iloc[0,0]
    #print(assay_name)
    df = df.iloc[:,1]
    #get emission fraction data from the 3d tab (use Coking Refinery)  
    emission = pd.read_excel(file,sheet_name=2, header = None)

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

def midstream_extraction(fpath):
    '''Extrac midstream product slates and emissions data given
    Inputs: filepath for 100yr or 20yr GWP prelim run results
    Outputs: a dataframe of assay library'''

    assay_file_list = dict()
    for directory in fpath:
        folder = directory.split('/')[-1].split(' ')[0].split(' ')[0].lower()
        assay_file_list[folder] = []
        
        for file in os.listdir(directory):
            filename = os.fsdecode(file)
            if filename.endswith('.xlsx'): 
    #            print(filename)
                assay_file_list[folder].append(join(directory, filename))


    # The parameter names are extracted into a series 
    parameter = pd.read_excel(assay_file_list['oci'][0],sheet_name=0, header=None)
    parameter = parameter.iloc[:,0]

    parameter = pd.concat([parameter,pd.DataFrame(['emission_frac_CO2','emission_frac_CH4','emission_frac_N2O'])],axis =0)
    parameter = parameter.reset_index().drop(columns ='index')



    parameter.iloc[0,0]='parameter'
    parameter.iloc[1,0]='Default Refinery'
    parameter.iloc[2,0]='assay_id'
    # store all one hundred year assay product slates in a dictionary with assay group as keys and assay product slate dataframes as values
    assay_slates_df = dict()
    for assay_group in assay_file_list:
        #print(assay_group)
        df_list = []
        for assay_file in assay_file_list[assay_group]:
            df_list.append(extract_product_slate(assay_file))        
        df =pd.concat([parameter,pd.concat(df_list,axis = 1)],axis=1)
        df.columns = df.iloc[0] 
        df = df[1:]
        df = df.T
        df.columns = df.iloc[0]
        df = df[1:]
        df = df.astype('float',errors='ignore')
        #df['assay_group']=assay_group
        assay_slates_df[assay_group]= df    

    assay_library = pd.concat(assay_slates_df,axis = 0)

    assay_library.reset_index(inplace = True)


    assay_library.rename(columns={'level_0':'assay_group',0:'assay_name'},inplace = True)



    #Take the first assay of duplicate assays in each assay group

    #assay_library = assay_library.groupby(['assay_group','assay_name']).first()



    assay_library = assay_library.dropna(axis = 1,how ='all')

    assay_library.reset_index(inplace = True)

    assay_library.drop_duplicates(subset='assay_name',keep='first')

    assay_library['assay_name'] = assay_library['assay_name'].apply(lambda x: x.strip())
    assay_library.drop(columns = 'index',inplace = True)
    return assay_library

onehundred_df = midstream_extraction(onehundredyr_path)
onehundred_df['gwp'] = '100'
twenty_df = midstream_extraction(twentyyr_path)
twenty_df['gwp'] = '20'
final_assay_library = pd.concat([onehundred_df, twenty_df]).reset_index().drop(columns = 'index')
# Use the old twenty year file names to get correct assay names. Reason: 100 year file name is not clean. 
assay_name_path = {'haverly': sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/Old Results/Haverly 20y',
                          'oci': sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/Old Results/OCI 20y',
                          'prelim': sp_dir + '/Midstream/Liam_Batchrun/OCI 3.0 (20-y GWP)/Old Results/PRELIM 20y'}                              

def assay_name_20yr(assay_group,assay_id):
    '''return the right assay name based on the file names in the old 20 year direcotry.'''
    for filename in os.listdir(assay_name_path[assay_group]):
        if filename.split('_')[0]==assay_id:
            assay_name = '_'.join(filename.split('_')[1:])[:-5]
            return assay_name.strip()

final_assay_library['assay_name']=final_assay_library.apply(lambda x: assay_name_20yr(x['assay_group'],x['assay_id']),axis=1)


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
    assay_gravity = []
    df = pd.DataFrame()
    for i in range(assay_files[assay][1]):
        assay_name.append(assay_df.iloc[i*15,0].strip())
        assay_throughput.append(float(assay_df.iloc[3+i*15,2]))
        assay_sulfur.append(float(assay_df.iloc[6+i*15,2]))
        assay_gravity.append(float(assay_df.iloc[8+i*15,2]))
    df['assay_name']=assay_name
    df['throughput']=assay_throughput
    df['sulfur']=assay_sulfur
    df['gravity']=assay_gravity
    assay_bbl_sulfur[assay]=df
    

assay_bbl_sulfur_library = pd.concat(assay_bbl_sulfur,axis = 0)

assay_bbl_sulfur_library.reset_index(level = 0, inplace = True)

assay_bbl_sulfur_library.rename(columns ={'level_0':'assay_group'},inplace = True)

assay_bbl_sulfur_library.reset_index(inplace = True)

assay_bbl_sulfur_library.drop(columns = 'index',inplace = True)

#Take the first assay of duplicate assays in each assay group
assay_bbl_sulfur_library = assay_bbl_sulfur_library.groupby(['assay_group','assay_name']).first()

assay_bbl_sulfur_library.reset_index(inplace = True)


#assay_bbl_sulfur_library.to_excel(sp_dir + '/Midstream/Liam_Batchrun/Analytics/assay_bbl_sulfur_library.xlsx',index = False)

final_assay_library_merged = final_assay_library.merge(assay_bbl_sulfur_library,how = 'left',indicator = True)


if final_assay_library_merged[final_assay_library_merged['_merge']!='both'].shape[0]>0:
    print('unmerged, check results.')
else:
    final_assay_library_merged.drop(columns = '_merge')


final_assay_library_merged.drop(columns = '_merge',inplace = True)
final_assay_library_merged.to_excel(sp_dir + '/Midstream/Liam_Batchrun/Analytics/final_assay_library.xlsx',index = False)


# ## Mapping OPGEE modelled fields to OCI, PRELIM and Haverly Assays based on API gravity and Surfur content


pub_data = pd.read_excel(sp_dir + '/Upstream/Public Data Batch runs/Scraped Public Data.xlsx')
field_assay = pub_data[['Field location (Country)', 'Field name', 'Assay Name']].dropna()


midstream = field_assay.merge(final_assay_library_merged,left_on = 'Assay Name', right_on = 'assay_name',how = 'left', indicator = True)
# only select one assay if there are multiple assays with the same name matched to the field

midstream = midstream.groupby(['Field name','gwp']).first()
midstream.to_csv(sp_dir+'/Upstream/upstream_data_pipeline_sp/Processed_Outputs/midstream_postprocessed.csv')