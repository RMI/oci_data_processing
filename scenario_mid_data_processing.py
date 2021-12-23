import pandas as pd
import os
from os.path import join    

sp_dir= '/Users/rwang/RMI/Climate Action Engine - Documents/OCI Phase 2'

print('Extracting product slates and emission data from Liam batch run results...')
nondefault_directory_path = {'haverly': sp_dir +'/Deep Dive page/Midstream/nondefault runs new/haverly 20y',
                              'oci':sp_dir + '/Deep Dive page/Midstream/nondefault runs new/oci 20y',
                              'opem':sp_dir + '/Deep Dive page/Midstream/nondefault runs new/prelim 20y'}

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


# The parameter names are extracted into a series 
parameter = pd.read_excel(nondefault_assay_file_list['oci'][0],sheet_name=0, header=None)
parameter = parameter.iloc[:,0]

parameter = pd.concat([parameter,pd.DataFrame(['emission_frac_CO2','emission_frac_CH4','emission_frac_N2O'])],axis =0)
parameter = parameter.reset_index().drop(columns ='index')

parameter.iloc[0,0]='parameter'
parameter.iloc[1,0]='Default Refinery'
parameter.iloc[2,0]='assay_id'


