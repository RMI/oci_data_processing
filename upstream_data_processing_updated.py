#!/usr/bin/env python
# coding: utf-8

# ## Import Packages

# In[1]:


import pandas as pd
import numpy as np
import os
import glob
import sqlite3


# ## Set up for Data Import

# In[2]:


# Get the directory of all csv files
d = '/Users/lschmeisser/RMI/Climate Action Engine - Documents/OCI Phase 2/Upstream/upstream_data_pipeline_sp/Outputs/'     #path to folder where files are located
os.chdir(d)                                                                                   #change directory to path


# In[3]:


#Define column names for results csvs
column_names =  [    
'Downhole pump',
 'Water reinjection ',
 'Natural gas reinjection',
 'Water flooding',
 'Gas lifting',
 'Gas flooding',
 'Steam flooding',
 'Oil sands mine (integrated with upgrader)',
 'Oil sands mine (non-integrated with upgrader)',
 'Field location (Country)',
 'Field_name',
 'Field age',
 'Field depth',
 'Oil production volume',
 'Number of producing wells',
 'Number of water injecting wells',
 'Production tubing diameter',
 'Productivity index',
 'Reservoir pressure',
 'Reservoir temperature',
 'Offshore?',
 'API gravity',
 'Gas composition N2',
 'Gas composition CO2',
 'Gas composition C1',
 'Gas composition C2',
 'Gas composition C3',
 'Gas composition C4+',
 'Gas composition H2S',
 'Gas-to-oil ratio (GOR)',
 'Water-to-oil ratio (WOR)',
 'Water injection ratio',
 'Gas lifting injection ratio',
 'Gas flooding injection ratio',
 'Flood gas ',
 'Liquids unloading practice',
 'Fraction of CO2 breaking through to producers',
 'Source of makeup CO2',
 'Percentage of sequestration credit assigned to the oilfield',
 'Steam-to-oil ratio (SOR)',
 'Fraction of required electricity generated onsite',
 'Fraction of remaining natural gas reinjected',
 'Fraction of produced water reinjected',
 'Fraction of steam generation via cogeneration ',
 'Fraction of steam generation via solar thermal',
 'Heater/treater',
 'Stabilizer column',
 'Upgrader type',
 'Associated Gas Processing Path',
 'Flaring-to-oil ratio',
 'Venting-to-oil ratio (purposeful)',
 'Volume fraction of diluent',
 'Low carbon richness (semi-arid grasslands)',
 'Moderate carbon richness (mixed)',
 'High carbon richness (forested)',
 'Low intensity development and low oxidation',
 'Moderate intensity development and moderate oxidation',
 'High intensity development and high oxidation',
 'Ocean tanker',
 'Barge',
 'Pipeline',
 'Rail',
 'Truck',
 'Transport distance (one way) - Ocean tanker',
 'Transport distance (one way) - Barge',
 'Transport distance (one way) - Pipeline',
 'Transport distance (one way) - Rail',
 'Transport distance (one way) - Truck',
 'Ocean tanker size, if applicable',
 'Small sources emissions',
 'e-Total energy consumption','e-Total GHG emissions', 
 'e-Total GHG emissions-Combustion/land use','e-Total GHG emissions-VFF',
 'd-Total energy consumption','d-Total GHG emissions', 
 'd-Total GHG emissions-Combustion/land use','d-Total GHG emissions-VFF',
 'p-Total energy consumption','p-Total GHG emissions', 
 'p-Total GHG emissions-Combustion/land use','p-Total GHG emissions-VFF',
 's-Total energy consumption','s-Total GHG emissions', 
 's-Total GHG emissions-Combustion/land use','s-Total GHG emissions-VFF',
 'l-Total energy consumption','l-Total GHG emissions', 
 'l-Total GHG emissions-Combustion/land use','l-Total GHG emissions-VFF',
 'm-Total energy consumption','m-Total GHG emissions', 
 'm-Total GHG emissions-Combustion/land use','m-Total GHG emissions-VFF',
 'w-Total energy consumption','w-Total GHG emissions', 
 'w-Total GHG emissions-Combustion/land use','w-Total GHG emissions-VFF',
 't-Total energy consumption','t-Total GHG emissions', 
 't-Total GHG emissions-Combustion/land use','t-Total GHG emissions-VFF','t-Loss factor',
 'g-Total energy consumption','g-Total GHG emissions', 
 'g-Total GHG emissions-Combustion/land use','g-Total GHG emissions-VFF',
 'Other small sources','Offsite emissions credit/debit','Lifecycle energy consumption',
 'CSS-Total CO2 sequestered','Lifecycle GHG emissions','Field-by-field check']


# ## Process 'Results' csvs

# In[4]:


#Create list of all results csvs
list_results = sorted(glob.glob('*Results*.csv', recursive=True))                             #list all results .csvs
len(list_results)                                                                             #how many results files available


# In[5]:


#Define a function called 'clean_df' that goes through excel fil and grabs the rows/columns where results are stored
#Then transpose the matrix, assign proper column names, and drop any rows that don't have information and return the clean dataframe
def clean_df(df,column_names):
    '''clean the df and transpose to map the column names'''
    df = df.iloc[: , 7:]
    df = df.iloc[[8,9,10,11,12,13,14,15,16,19,20,21,22,23,24,25,26,27,28,29,30,33,35,36,37,38,39,40,41,45,46,47,48,49,
         50,54,57,58,61,62,63,64,65,66,67,69,70,71,76,85,86,87,91,92,93,95,96,97,101,102,103,104,105,107,108,109,110,
        111,112,114,129,130,131,132,135,136,137,138,141,142,143,144,147,148,149,150,153,154,155,156,159,160,161,162,
                 165,166,167,168,171,172,173,174,175,178,179,180,181,183,185,187,190,192,194]]
    df_t = df.transpose()
    df_t.columns = column_names
    df_t = df_t.dropna(how = 'all')
    return df_t


# In[6]:


#Create an empty list in which to store cleaned results
#Loop through each results csv, clean it, add in informatoin about year, field_type, frack, lng, gwp as scraped from file name
#If this doesn't work, print the file name as a 'problematic file' in the try/except loop
list_df =[]
for file in list_results:
    try:
        df = pd.read_csv(d+file,header = None)  
        result = clean_df(df,column_names)
        result['original_file']=file
        result['year']=file.split('_')[3]
        result['field_type']=file.split('_')[4].lower()
        result['frack?']= True if file.split('_')[5].lower()=='frack' else False
        result['lng?'] = True if file.split('_')[6].lower()=='lng' else False
        result['gwp'] = file.split('_')[7][3:-4].lower()
        result['Field_name'] = file.split('_')[0]
        list_df.append(result)
    except:
        print("problematic file: " + file)
        print(list_results.index(file))


# In[7]:


results_df = pd.concat(list_df)


# In[8]:


#List out all numerical columns to convert to type float
numerical_columns = [
 'Field age',
 'Field depth',
 'Oil production volume',
 'Number of producing wells',
 'Number of water injecting wells',
 'Production tubing diameter',
 'Productivity index',
 'Reservoir pressure',
 'Reservoir temperature',
 'Offshore?',
 'API gravity',
 'Gas composition N2',
 'Gas composition CO2',
 'Gas composition C1',
 'Gas composition C2',
 'Gas composition C3',
 'Gas composition C4+',
 'Gas composition H2S',
 'Gas-to-oil ratio (GOR)',
 'Water-to-oil ratio (WOR)',
 'Water injection ratio',
 'Gas lifting injection ratio',
 'Gas flooding injection ratio',
 'Flood gas ',
 'Liquids unloading practice',
 'Fraction of CO2 breaking through to producers',
 'Source of makeup CO2',
 'Percentage of sequestration credit assigned to the oilfield',
 'Steam-to-oil ratio (SOR)',
 'Fraction of required electricity generated onsite',
 'Fraction of remaining natural gas reinjected',
 'Fraction of produced water reinjected',
 'Fraction of steam generation via cogeneration ',
 'Fraction of steam generation via solar thermal',
 'Heater/treater',
 'Stabilizer column',
 'Upgrader type',
 'Associated Gas Processing Path',
 'Flaring-to-oil ratio',
 'Venting-to-oil ratio (purposeful)',
 'Volume fraction of diluent',
 'Low carbon richness (semi-arid grasslands)',
 'Moderate carbon richness (mixed)',
 'High carbon richness (forested)',
 'Low intensity development and low oxidation',
 'Moderate intensity development and moderate oxidation',
 'High intensity development and high oxidation',
 'Ocean tanker',
 'Barge',
 'Pipeline',
 'Rail',
 'Truck',
 'Transport distance (one way) - Ocean tanker',
 'Transport distance (one way) - Barge',
 'Transport distance (one way) - Pipeline',
 'Transport distance (one way) - Rail',
 'Transport distance (one way) - Truck',
 'Ocean tanker size, if applicable',
 'Small sources emissions',
 'e-Total energy consumption',
 'e-Total GHG emissions',
 'e-Total GHG emissions-Combustion/land use',
 'e-Total GHG emissions-VFF',
 'd-Total energy consumption',
 'd-Total GHG emissions',
 'd-Total GHG emissions-Combustion/land use',
 'd-Total GHG emissions-VFF',
 'p-Total energy consumption',
 'p-Total GHG emissions',
 'p-Total GHG emissions-Combustion/land use',
 'p-Total GHG emissions-VFF',
 's-Total energy consumption',
 's-Total GHG emissions',
 's-Total GHG emissions-Combustion/land use',
 's-Total GHG emissions-VFF',
 'l-Total energy consumption',
 'l-Total GHG emissions',
 'l-Total GHG emissions-Combustion/land use',
 'l-Total GHG emissions-VFF',
 'm-Total energy consumption',
 'm-Total GHG emissions',
 'm-Total GHG emissions-Combustion/land use',
 'm-Total GHG emissions-VFF',
 'w-Total energy consumption',
 'w-Total GHG emissions',
 'w-Total GHG emissions-Combustion/land use',
 'w-Total GHG emissions-VFF',
 't-Total energy consumption',
 't-Total GHG emissions',
 't-Total GHG emissions-Combustion/land use',
 't-Total GHG emissions-VFF',
 't-Loss factor',
 'g-Total energy consumption',
 'g-Total GHG emissions',
 'g-Total GHG emissions-Combustion/land use',
 'g-Total GHG emissions-VFF',
 'Other small sources',
 'Offsite emissions credit/debit',
 'Lifecycle energy consumption',
 'CSS-Total CO2 sequestered',
 'Lifecycle GHG emissions']


# In[9]:


#more clean up of dataframe
results_df = results_df.replace(r'^\s+$', np.nan, regex=True)               #replace empty strings with NA
results_df = results_df.replace(r'\\', np.nan, regex=True)                  #
results_df.reset_index(inplace = True, drop=True)                           #reset index and drop index column
results_df[numerical_columns]= results_df[numerical_columns].astype(float)  #set these numerica columns as type float
results_df['Field_name']=results_df['Field_name'].apply(lambda x: x.strip())#strip whitespace from field names


# In[10]:


#double check there are no spaces in field names
results_df['Field_name'] = results_df['Field_name'].replace(" ", "")


# ## Process 'Energy Summary' csvs

# In[11]:


#Grab energy summary csvs
list_energysummary = sorted(glob.glob('*Energy*.csv', recursive=True))   #list all energy summary .csvs
len(list_energysummary)                                                  #how many energy summary files - should match number of results csvs


# In[12]:


#Create empty lists to populate with energy summary data
ES_MJperd =[]
ES_mmbtuperd = []
ES_Energy_Density_crude_oil = []
ES_Energy_Density_petcoke = []
ES_Energy_Density_C2 = []
ES_Energy_Density_C3 = []
ES_Energy_Density_C4 = []
ES_Crude_output = []
ES_Gas_output = []
ES_NGL_output = []
ES_Gas_output_MJ = []
ES_Petcoke_fuel =[]
Field_name = []
original_file = []
gwp = []


# In[13]:


for file in list_energysummary:
    df = pd.read_csv(d+file,header=None)
    ES_MJperd.append(float(df.iloc[127,5]))
    ES_mmbtuperd.append(float(df.iloc[127,4]))
    ES_Energy_Density_crude_oil.append(float(df.iloc[132,12]))
    ES_Energy_Density_petcoke.append(float(df.iloc[134,12]))
    ES_Energy_Density_C2.append(float(df.iloc[140,12]))
    ES_Energy_Density_C3.append(float(df.iloc[141,12]))
    ES_Energy_Density_C4.append(float(df.iloc[142,12]))
   
    ES_Crude_output.append(float(df.iloc[88,4]))
    ES_Gas_output.append(float(df.iloc[84,4]))
    
    if df.iloc[120,3] == 'Gas':
        ES_Gas_output_MJ.append(float(df.iloc[120,5]))
    else:
        ES_Gas_output_MJ.append(float(df.iloc[123,5]))
        
    ES_NGL_output.append(float(df.iloc[86,4]))
    ES_Petcoke_fuel.append(float(df.iloc[76,4]))
    Field_name.append(file.split('_')[0])
    #original_file.append(file)
    gwp.append(file.split('_')[7][3:-4].lower())


# In[14]:


#combine lists of values into dataframe
energysummary_df = pd.DataFrame({'Field_name':Field_name,'gwp':gwp, #'original_file':original_file
                               'ES_MJperd':ES_MJperd,'ES_mmbtuperd':ES_mmbtuperd,
                               'ES_Energy_Density_crude(mmbtu/t)':ES_Energy_Density_crude_oil,'ES_Energy_Density_petcoke(mmbtu/t)':ES_Energy_Density_petcoke,
                              'ES_Energy_Density_C2(mmbtu/t)':ES_Energy_Density_C2,'ES_Energy_Density_C3(mmbtu/t)':ES_Energy_Density_C3,
                               'ES_Energy_Density_C4(mmbtu/t)':ES_Energy_Density_C4, 'ES_Crude_output(mmbut/d)':ES_Crude_output,
                              'ES_Gas_output(mmbtu/d)':ES_Gas_output, 'ES_NGL_output(mmbtu/d)':ES_NGL_output,
                              'ES_Gas_output(MJ/d)':ES_Gas_output_MJ,'ES_Petcoke_fuel(mmbtu/d)':ES_Petcoke_fuel})


# In[15]:


#double check there are no spaces in field names
energysummary_df['Field_name'] = energysummary_df['Field_name'].replace(" ", "")


# ## Process 'VFF' csvs

# In[16]:


#we want to grab both co2 and ch4 emissions from vff csvs


# In[17]:


#Grab vff csvs
list_vff = sorted(glob.glob('*VFF*.csv', recursive=True))   #list all VFF .csvs
len(list_vff)


# In[18]:


#Create empty lists in which to fill in vff data
venting_ch4 =[]
venting_ch4_miq = []
venting_ch4_uponly = []
fugitive_ch4 =[]
flaring_ch4 = []
fugitive_ch4_miq = []
fugitive_ch4_uponly = []
venting_production_ch4 = []
venting_gatherboostprocesss_ch4 = []
venting_transmissionstorage_ch4 = []
venting_2ndproduction_ch4 = []
venting_enduse_ch4 = []
fugitive_production_ch4 = []
fugitive_gatherboostprocesss_ch4 = []
fugitive_transmissionstorage_ch4 =[]
fugitive_2ndproduction_ch4 = []
fugitive_enduse_ch4 = []
venting_co2 = []
fugitive_co2 = []
Field_name = []
original_file = []
gwp = []


# In[19]:


#fill in empty lists with data from vff files
for file in list_vff:
    df = pd.read_csv(d+file,header=None)
    venting_ch4.append(sum(df.iloc[87:134,9].apply(lambda x:float(x))))
    fugitive_ch4.append(sum(df.iloc[87:133,10].apply(lambda x:float(x))))
    flaring_ch4.append(df.iloc[133,10])                                     #always going to be K134
    venting_co2.append(sum(df.iloc[87:134,7].apply(lambda x:float(x))))
    fugitive_co2.append(sum(df.iloc[87:134,8].apply(lambda x:float(x))))
    venting_production_ch4.append(sum(df.iloc[87:107,9].apply(lambda x:float(x))))
    venting_gatherboostprocesss_ch4.append(sum(df.iloc[107:112,9].apply(lambda x:float(x))))
    venting_transmissionstorage_ch4.append(sum(df.iloc[112:117,9].apply(lambda x:float(x))))
    venting_2ndproduction_ch4.append(sum(df.iloc[123:133,9].apply(lambda x:float(x))))
    venting_enduse_ch4.append(float(df.iloc[122,9]))
    fugitive_production_ch4.append(sum(df.iloc[87:107,10].apply(lambda x:float(x))))
    fugitive_gatherboostprocesss_ch4.append(sum(df.iloc[107:112,10].apply(lambda x:float(x))))
    fugitive_transmissionstorage_ch4.append(sum(df.iloc[112:117,10].apply(lambda x:float(x))))
    fugitive_2ndproduction_ch4.append(sum(df.iloc[123:133,10].apply(lambda x:float(x))))
    fugitive_enduse_ch4.append((float(df.iloc[122,10])))
    venting_ch4_miq= [sum(x) for x in zip(venting_production_ch4, venting_2ndproduction_ch4)]
    fugitive_ch4_miq= [sum(x) for x in zip(fugitive_production_ch4, fugitive_2ndproduction_ch4)]
    venting_ch4_uponly = [sum(x) for x in zip(venting_production_ch4,venting_gatherboostprocesss_ch4,venting_2ndproduction_ch4)]
    fugitive_ch4_uponly = [sum(x) for x in zip(fugitive_production_ch4,fugitive_gatherboostprocesss_ch4,fugitive_2ndproduction_ch4)]
    Field_name.append(file.split('_')[0])
    #original_file.append(file)
    gwp.append(file.split('_')[7][3:-4].lower())


# In[20]:


#combine lists of data into dataframe
vff_df = pd.DataFrame({'Field_name':Field_name, 'gwp':gwp, #'original_file':original_file,
                   'venting_ch4(t/d)':venting_ch4,'fugitive_ch4(t/d)':fugitive_ch4,
                   'flaring_ch4(t/d)':flaring_ch4,'venting_co2(t/d)':venting_co2,'fugitive_co2(t/d)':fugitive_co2,
                   'venting_ch4_miq(t/d)':venting_ch4_miq,'fugitive_ch4_miq(t/d)':fugitive_ch4_miq,
                   'venting_ch4_uponly(t/d)':venting_ch4_uponly,'fugitive_ch4_uponly(t/d)':fugitive_ch4_uponly,
                   'ch4_production(t/d)': [sum(x) for x in zip(venting_production_ch4,fugitive_production_ch4)],
                   'ch4_gatherboostprocess(t/d)': [sum(x) for x in zip(venting_gatherboostprocesss_ch4,fugitive_gatherboostprocesss_ch4)],
                   'ch4_transmissionstorage(t/d)': [sum(x) for x in zip(venting_transmissionstorage_ch4,fugitive_transmissionstorage_ch4)],
                   'ch4_2ndproduction(t/d)':[sum(x) for x in zip(venting_2ndproduction_ch4,fugitive_2ndproduction_ch4)],
                   'ch4_enduse(t/d)':[sum(x) for x in zip(venting_enduse_ch4,fugitive_enduse_ch4)]})


# In[21]:


#add in new columns for tCH4/year and tCH4/year-miQ
vff_df['tCH4/year'] = (vff_df['flaring_ch4(t/d)'].astype(float)+vff_df['venting_ch4(t/d)']+vff_df['fugitive_ch4(t/d)'])*365
vff_df['tCH4/year-miQ']=(vff_df['flaring_ch4(t/d)'].astype(float)+vff_df['venting_ch4_miq(t/d)']+vff_df['fugitive_ch4_miq(t/d)'])*365


# In[22]:


#double check field names don't have spaces
vff_df['Field_name'] = vff_df['Field_name'].replace(" ", "")


# ## Add in data from 'Flow' csvs

# In[23]:


#Grab flow sheet csvs
list_flow = sorted(glob.glob('*Flow*.csv', recursive=True))   #list all Flow .csvs
len(list_flow)


# In[24]:


#Create empty lists in which to populate data from csvs
FS_LPG_export_LPG = [] #Flow Sheet!W9
FS_LPG_export_C2 = [] #W17
FS_LPG_export_C3 = [] #W18
FS_LPG_export_C4  = [] #W19 
FS_Ethane_to_Petchem = [] #CP17
FS_Petcoke_to_stock =[]
FS_Gas_at_Wellhead =[] #AF24
Field_name = []
original_file = []
gwp = []


# In[25]:


#fill lists with data from flow csvs
Field_name = []
original_file = []
for file in list_flow:
    df = pd.read_csv(d+file,header=None)
    FS_LPG_export_LPG.append(float(df.iloc[8,22]))
    FS_LPG_export_C2.append(float(df.iloc[16,22]))
    FS_LPG_export_C3.append(float(df.iloc[17,22]))
    FS_LPG_export_C4.append(float(df.iloc[18,22]))
    FS_Ethane_to_Petchem.append(float(df.iloc[16,93]))
    FS_Petcoke_to_stock.append(float(df.iloc[6,214]))
    FS_Gas_at_Wellhead.append(float(df.iloc[23,31]))
    Field_name.append(file.split('_')[0])
    original_file.append(file)
    gwp.append(file.split('_')[7][3:-4].lower())


# In[26]:


#Create dataframe for flow data
flowsheet_df = pd.DataFrame({'Field_name':Field_name, 'gwp':gwp, #'original_file':original_file,
                   'FS_LPG_export_LPG(t/d)':FS_LPG_export_LPG,'FS_LPG_export_C2(t/d)':FS_LPG_export_C2,
                   'FS_LPG_export_C3(t/d)': FS_LPG_export_C3, 'FS_LPG_export_C4(t/d)':FS_LPG_export_C4,
                          'FS_Ethane_to_Petchem(t/d)':FS_Ethane_to_Petchem,
                         'FS_Petcoke_to_stock(t/d)':FS_Petcoke_to_stock,'FS_Gas_at_Wellhead(t/d)':FS_Gas_at_Wellhead})


# In[27]:


#double check no spaces in field names
flowsheet_df['Field_name'] = flowsheet_df['Field_name'].replace(" ", "")


# ## Merge Results, Energy Summary, VFF, and Flow into one dataframe

# In[28]:


#merge results and energysummary
merge = results_df.merge(energysummary_df, on=['Field_name','gwp'], how = 'outer')


# In[29]:


#merge in vff
merge = merge.merge(vff_df, on=['Field_name','gwp'], how='outer')


# In[30]:


#merge in flowsheet
merge = merge.merge(flowsheet_df, on=['Field_name','gwp'], how='outer')


# In[31]:


#Add new column for tco2e/yr after all data are merged
merge['tCO2e/yr']=merge['Lifecycle GHG emissions']*merge['ES_MJperd']/10**6*365


# ## Check against upstream results

# In[32]:


#read in upstream_results and make sure we aren't missing any columns here
sp_dir = '/Users/lschmeisser/RMI/Climate Action Engine - Documents/OCI Phase 2'
connection = sqlite3.connect(sp_dir+"/OCI_Database.db")
up_mid_down = pd.read_sql('select * from upstream_results',connection)


# In[33]:


list_up = up_mid_down.columns.to_list()
list_merge = merge.columns.to_list()


# In[34]:


def Diff(li1, li2):
    return list(set(li1) - set(li2)) + list(set(li2) - set(li1))


# In[35]:


print(Diff(list_up,list_merge))


# ## Export results to csv or database

# In[36]:


pd.set_option('display.max_columns', None)


# In[37]:


merge.to_csv('/Users/lschmeisser/RMI/Climate Action Engine - Documents/OCI Phase 2/Upstream/upstream_data_pipeline_sp/Processed_Outputs/upstream_postprocessed.csv', index=False)


# ## Create a spreadsheet that is easier to view (field name and important variables to the left)

# In[39]:


easyview = merge[['Field_name','Field location (Country)','year',
'field_type',
'frack?',
'lng?',
'gwp',
'Oil production volume', 
'Field age',
'Field depth',
'Downhole pump',
'Water reinjection ',
'Natural gas reinjection',
'Water flooding',
'Gas lifting',
'Gas flooding',
'Steam flooding',
'Oil sands mine (integrated with upgrader)',
'Oil sands mine (non-integrated with upgrader)',
'Number of producing wells',
'Number of water injecting wells',
'Production tubing diameter',
'Productivity index',
'Reservoir pressure',
'Reservoir temperature',
'Offshore?',
'API gravity',
'Gas composition N2',
'Gas composition CO2',
'Gas composition C1',
'Gas composition C2',
'Gas composition C3',
'Gas composition C4+',
'Gas composition H2S',
'Gas-to-oil ratio (GOR)',
'Water-to-oil ratio (WOR)',
'Water injection ratio',
'Gas lifting injection ratio',
'Gas flooding injection ratio',
'Flood gas ',
'Liquids unloading practice',
'Fraction of CO2 breaking through to producers',
'Source of makeup CO2',
'Percentage of sequestration credit assigned to the oilfield',
'Steam-to-oil ratio (SOR)',
'Fraction of required electricity generated onsite',
'Fraction of remaining natural gas reinjected',
'Fraction of produced water reinjected',
'Fraction of steam generation via cogeneration ',
'Fraction of steam generation via solar thermal',
'Heater/treater',
'Stabilizer column',
'Upgrader type',
'Associated Gas Processing Path',
'Flaring-to-oil ratio',
'Venting-to-oil ratio (purposeful)',
'Volume fraction of diluent',
'Low carbon richness (semi-arid grasslands)',
'Moderate carbon richness (mixed)',
'High carbon richness (forested)',
'Low intensity development and low oxidation',
'Moderate intensity development and moderate oxidation',
'High intensity development and high oxidation',
'Ocean tanker',
'Barge',
'Pipeline',
'Rail',
'Truck',
'Transport distance (one way) - Ocean tanker',
'Transport distance (one way) - Barge',
'Transport distance (one way) - Pipeline',
'Transport distance (one way) - Rail',
'Transport distance (one way) - Truck',
'Ocean tanker size, if applicable',
'Small sources emissions',
'e-Total energy consumption',
'e-Total GHG emissions',
'e-Total GHG emissions-Combustion/land use',
'e-Total GHG emissions-VFF',
'd-Total energy consumption',
'd-Total GHG emissions',
'd-Total GHG emissions-Combustion/land use',
'd-Total GHG emissions-VFF',
'p-Total energy consumption',
'p-Total GHG emissions',
'p-Total GHG emissions-Combustion/land use',
'p-Total GHG emissions-VFF',
's-Total energy consumption',
's-Total GHG emissions',
's-Total GHG emissions-Combustion/land use',
's-Total GHG emissions-VFF',
'l-Total energy consumption',
'l-Total GHG emissions',
'l-Total GHG emissions-Combustion/land use',
'l-Total GHG emissions-VFF',
'm-Total energy consumption',
'm-Total GHG emissions',
'm-Total GHG emissions-Combustion/land use',
'm-Total GHG emissions-VFF', 
'w-Total energy consumption',
'w-Total GHG emissions',
'w-Total GHG emissions-Combustion/land use',
'w-Total GHG emissions-VFF',
't-Total energy consumption',
't-Total GHG emissions',
't-Total GHG emissions-Combustion/land use',
't-Total GHG emissions-VFF',
't-Loss factor',
'g-Total energy consumption',
'g-Total GHG emissions',
'g-Total GHG emissions-Combustion/land use',
'g-Total GHG emissions-VFF',
'Other small sources',
'Offsite emissions credit/debit',
'Lifecycle energy consumption',
'CSS-Total CO2 sequestered',
'Lifecycle GHG emissions',
'Field-by-field check',
'ES_MJperd',
'ES_mmbtuperd',
'ES_Energy_Density_crude(mmbtu/t)',
'ES_Energy_Density_petcoke(mmbtu/t)',
'ES_Energy_Density_C2(mmbtu/t)',
'ES_Energy_Density_C3(mmbtu/t)',
'ES_Energy_Density_C4(mmbtu/t)',
'ES_Crude_output(mmbut/d)',
'ES_Gas_output(mmbtu/d)',
'ES_NGL_output(mmbtu/d)',
'ES_Gas_output(MJ/d)',
'ES_Petcoke_fuel(mmbtu/d)',
'venting_ch4(t/d)',
'fugitive_ch4(t/d)',
'flaring_ch4(t/d)',
'venting_co2(t/d)',
'fugitive_co2(t/d)',
'venting_ch4_miq(t/d)',
'fugitive_ch4_miq(t/d)',
'venting_ch4_uponly(t/d)',
'fugitive_ch4_uponly(t/d)',
'ch4_production(t/d)',
'ch4_gatherboostprocess(t/d)',
'ch4_transmissionstorage(t/d)',
'ch4_2ndproduction(t/d)',
'ch4_enduse(t/d)',
'tCH4/year',
'tCH4/year-miQ',
'FS_LPG_export_LPG(t/d)',
'FS_LPG_export_C2(t/d)',
'FS_LPG_export_C3(t/d)',
'FS_LPG_export_C4(t/d)',
'FS_Ethane_to_Petchem(t/d)',
'FS_Petcoke_to_stock(t/d)',
'FS_Gas_at_Wellhead(t/d)',
'tCO2e/yr']]


# In[40]:


#Write to excel file
easyview.to_excel('/Users/lschmeisser/RMI/Climate Action Engine - Documents/OCI Phase 2/Upstream/upstream_data_pipeline_sp/Processed_Outputs/easyview.xlsx', index=False)              

